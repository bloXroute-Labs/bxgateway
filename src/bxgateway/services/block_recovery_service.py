from bxcommon import constants
from bxcommon.utils import logger
from bxcommon.utils.expiration_queue import ExpirationQueue


class BlockRecoveryService(object):
    """
    Logic to handle scenario when gateway receives block with transaction sid and hash
    that it is not aware of.
    """

    def __init__(self, alarm_queue):
        self.alarm_queue = alarm_queue

        # block hash -> set of sids
        self.block_hash_to_sids = {}

        # block hash -> set of tx hashes
        self.block_hash_to_tx_hashes = {}

        self.block_hash_to_block = {}

        self.blocks_expiration_queue = ExpirationQueue(constants.MISSING_BLOCK_EXPIRE_TIME)

        self.sid_to_block_hash = {}
        self.tx_hash_to_block_hash = {}

        self.recovered_blocks = []

        self.cleanup_scheduled = False

    def add_block(self, block, block_hash, unknown_tx_sids, unknown_tx_contents):
        logger.debug("Block recovery: Tracking block with unknown txs. Block hash {0}".format(block_hash))

        self.block_hash_to_block[block_hash] = block

        self.blocks_expiration_queue.add(block_hash)

        self.block_hash_to_sids[block_hash] = set()
        self.block_hash_to_tx_hashes[block_hash] = set()

        for sid in unknown_tx_sids:
            self.sid_to_block_hash[sid] = block_hash
            self.block_hash_to_sids[block_hash].add(sid)

        for tx_hash in unknown_tx_contents:
            self.tx_hash_to_block_hash[tx_hash] = block_hash
            self.block_hash_to_tx_hashes[block_hash].add(tx_hash)

        self._schedule_cleanup()

    def check_missing_sid(self, sid):
        if sid in self.sid_to_block_hash:
            logger.debug("Block recovery: Received previously unknown tx sid {0}.".format(sid))

            block_hash = self.sid_to_block_hash[sid]

            if block_hash in self.block_hash_to_sids:
                if sid in self.block_hash_to_sids[block_hash]:
                    self.block_hash_to_sids[block_hash].discard(sid)

            del self.sid_to_block_hash[sid]

            self._check_if_recovered(block_hash)

    def check_missing_tx_hash(self, tx_hash):
        if tx_hash in self.tx_hash_to_block_hash:
            logger.debug("Block recovery: Received previously unknown tx hash {0}.".format(tx_hash))

            block_hash = self.tx_hash_to_block_hash[tx_hash]

            if block_hash in self.block_hash_to_tx_hashes:
                if tx_hash in self.block_hash_to_tx_hashes[block_hash]:
                    self.block_hash_to_tx_hashes[block_hash].discard(tx_hash)

            del self.tx_hash_to_block_hash[tx_hash]

            self._check_if_recovered(block_hash)

    def cancel_recovery_for_block(self, block_hash):
        if block_hash in self.block_hash_to_block:
            logger.debug("Block recovery: Received block {0} from. Stop tracking block unknown txs.".format(block_hash))
            self._remove_not_recovered_block(block_hash)

    def cleanup_old_blocks(self, clean_up_time=None):
        logger.debug("Block recovery: Running clean up task.")

        self.blocks_expiration_queue.remove_expired(current_time=clean_up_time,
                                                    remove_callback=self._remove_not_recovered_block)

        if self.block_hash_to_block:
            return constants.MISSING_BLOCK_EXPIRE_TIME

        # disable clean up until receive the next block with unknown tx
        self.cleanup_scheduled = False
        return 0

    def clean_up_recovered_blocks(self):
        logger.debug("Block recovery: Removing all of ready to retry blocks. {0} blocks."
                     .format(len(self.recovered_blocks)))
        del self.recovered_blocks[:]

    def _check_if_recovered(self, block_hash):
        if self._is_block_recovered(block_hash):
            logger.debug("Block recovery: Block {0} is ready for retry.".format(block_hash))
            block = self.block_hash_to_block[block_hash]
            self._remove_not_recovered_block(block_hash)
            self.recovered_blocks.append(block)

    def _is_block_recovered(self, block_hash):
        return len(self.block_hash_to_sids[block_hash]) == 0 and len(self.block_hash_to_tx_hashes[block_hash]) == 0

    def _remove_not_recovered_block(self, block_hash):
        if block_hash in self.block_hash_to_block:
            logger.debug("Block recovery: Removing block with hash {0}".format(block_hash))

            del self.block_hash_to_block[block_hash]

            for sid in self.block_hash_to_sids[block_hash]:
                if sid in self.sid_to_block_hash:
                    del self.sid_to_block_hash[sid]

            del self.block_hash_to_sids[block_hash]

            for tx_hash in self.block_hash_to_tx_hashes[block_hash]:
                if tx_hash in self.tx_hash_to_block_hash:
                    del self.tx_hash_to_block_hash[tx_hash]

            del self.block_hash_to_tx_hashes[block_hash]

    def _schedule_cleanup(self):
        if not self.cleanup_scheduled and self.block_hash_to_block:
            logger.debug("Block recovery: Scheduling unknown tx blocks clean up in {0} seconds."
                         .format(constants.MISSING_BLOCK_EXPIRE_TIME))
            self.alarm_queue.register_alarm(constants.MISSING_BLOCK_EXPIRE_TIME, self.cleanup_old_blocks)
            self.cleanup_scheduled = True
