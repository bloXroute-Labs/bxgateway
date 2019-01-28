import os
import time

from mock import MagicMock

from bxcommon import constants
from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils.mocks.mock_alarm_queue import MockAlarmQueue
from bxgateway.services.block_recovery_service import BlockRecoveryService


def _create_block():
    message_buffer = bytearray()
    message_buffer.extend(os.urandom(500))
    return message_buffer


class BlockRecoveryManagerTest(AbstractTestCase):

    def setUp(self):
        self.alarm_queue = MockAlarmQueue()
        self.block_recovery_service = BlockRecoveryService(self.alarm_queue)
        self.blocks = []
        self.block_hashes = []
        self.unknown_tx_sids = []
        self.unknown_tx_hashes = []

    def test_add_block(self):
        self._add_block()
        self._add_block(1)
        self._add_block(2)

    def test_check_missing_sid(self):
        self._add_block()

        sid = self.unknown_tx_sids[0][0]

        self.block_recovery_service.check_missing_sid(sid)

        self.assertEqual(len(self.block_recovery_service.block_hash_to_sids[self.block_hashes[0]]), 2)
        self.assertNotIn(sid, self.block_recovery_service.sid_to_block_hash)

    def test_check_missing_tx_hash(self):
        self._add_block()

        tx_hash = self.unknown_tx_hashes[0][0]

        self.block_recovery_service.check_missing_tx_hash(tx_hash)

        self.assertEqual(len(self.block_recovery_service.block_hash_to_tx_hashes[self.block_hashes[0]]), 1)
        self.assertNotIn(tx_hash, self.block_recovery_service.tx_hash_to_block_hash)

    def test_cancel_recovery_for_block(self):
        self._add_block()

        self.block_recovery_service.cancel_recovery_for_block(self.block_hashes[0])

        self.assertEqual(len(self.block_recovery_service.block_hash_to_block), 0)
        self.assertEqual(len(self.block_recovery_service.block_hash_to_sids), 0)
        self.assertEqual(len(self.block_recovery_service.block_hash_to_tx_hashes), 0)

        self.assertEqual(len(self.block_recovery_service.sid_to_block_hash), 0)
        self.assertEqual(len(self.block_recovery_service.tx_hash_to_block_hash), 0)

    def test_recovered_blocks__sids_arrive_first(self):
        self._add_block()

        # Missing sids arrive first
        for sid in self.unknown_tx_sids[0]:
            self.block_recovery_service.check_missing_sid(sid)

        # Then tx hashes arrive
        for tx_hash in self.unknown_tx_hashes[0]:
            self.block_recovery_service.check_missing_tx_hash(tx_hash)

        self.assertEqual(len(self.block_recovery_service.block_hash_to_block), 0)
        self.assertEqual(len(self.block_recovery_service.block_hash_to_sids), 0)
        self.assertEqual(len(self.block_recovery_service.block_hash_to_tx_hashes), 0)

        self.assertEqual(len(self.block_recovery_service.sid_to_block_hash), 0)
        self.assertEqual(len(self.block_recovery_service.tx_hash_to_block_hash), 0)

        self.assertEqual(len(self.block_recovery_service.recovered_blocks), 1)
        self.assertEqual(self.block_recovery_service.recovered_blocks[0], self.blocks[0])

    def test_recovered_blocks__tx_contents_arrive_first(self):
        self._add_block()

        # Missing tx hashes arrive first
        for tx_hash in self.unknown_tx_hashes[0]:
            self.block_recovery_service.check_missing_tx_hash(tx_hash)

        # Then missing sids arrive
        for sid in self.unknown_tx_sids[0]:
            self.block_recovery_service.check_missing_sid(sid)

        # Verify that no txs all blocks missing
        self.assertEqual(len(self.block_recovery_service.block_hash_to_block), 0)
        self.assertEqual(len(self.block_recovery_service.block_hash_to_sids), 0)
        self.assertEqual(len(self.block_recovery_service.block_hash_to_tx_hashes), 0)
        self.assertEqual(len(self.block_recovery_service.sid_to_block_hash), 0)
        self.assertEqual(len(self.block_recovery_service.tx_hash_to_block_hash), 0)

        # Verify that message is ready for retry
        self.assertEqual(len(self.block_recovery_service.recovered_blocks), 1)
        self.assertEqual(self.block_recovery_service.recovered_blocks[0], self.blocks[0])

    def test_clean_up_old_blocks__single_block(self):
        self.assertFalse(self.block_recovery_service.cleanup_scheduled)
        self.assertEqual(len(self.alarm_queue.alarms), 0)

        # Adding missing message
        self._add_block()

        # Verify that clean up is scheduled
        self.assertEqual(len(self.alarm_queue.alarms), 1)
        self.assertEqual(self.alarm_queue.alarms[0][0], constants.MISSING_BLOCK_EXPIRE_TIME)
        self.assertEqual(self.alarm_queue.alarms[0][1], self.block_recovery_service.cleanup_old_blocks)

        self.assertTrue(self.block_recovery_service.cleanup_scheduled)

        # Run clean up before message expires and check that it is still there
        self.block_recovery_service.cleanup_old_blocks(time.time() + constants.MISSING_BLOCK_EXPIRE_TIME / 2)
        self.assertEqual(len(self.block_recovery_service.block_hash_to_block), 1)
        self.assertTrue(self.block_recovery_service.cleanup_scheduled)

        # Run clean up after message expires and check that it is removed
        self.block_recovery_service.cleanup_old_blocks(time.time() + constants.MISSING_BLOCK_EXPIRE_TIME + 1)
        self.assertEqual(len(self.block_recovery_service.block_hash_to_block), 0)
        self.assertFalse(self.block_recovery_service.cleanup_scheduled)

    def test_clean_up_recovered_blocks(self):
        self.assertTrue(len(self.block_recovery_service.recovered_blocks) == 0)

        # Adding ready to retry messages
        self.block_recovery_service.recovered_blocks.append(_create_block())
        self.block_recovery_service.recovered_blocks.append(_create_block())
        self.block_recovery_service.recovered_blocks.append(_create_block())

        self.assertEqual(len(self.block_recovery_service.recovered_blocks), 3)

        # Removing ready to retry messages and verify that they are removed
        self.block_recovery_service.clean_up_recovered_blocks()

        self.assertEqual(len(self.block_recovery_service.recovered_blocks), 0)

    def test_clean_up_old_blocks__multiple_blocks(self):
        self.assertFalse(self.block_recovery_service.cleanup_scheduled)
        self.assertEqual(len(self.alarm_queue.alarms), 0)

        # Adding to blocks with 2 seconds difference between them
        self._add_block()
        time.time = MagicMock(return_value=time.time() + 3)
        self._add_block(1)

        # Verify that clean up scheduled
        self.assertEqual(len(self.alarm_queue.alarms), 1)
        self.assertEqual(self.alarm_queue.alarms[0][0], constants.MISSING_BLOCK_EXPIRE_TIME)
        self.assertEqual(self.alarm_queue.alarms[0][1], self.block_recovery_service.cleanup_old_blocks)

        # Verify that both blocks are there before the first one expires
        self.block_recovery_service.cleanup_old_blocks(time.time() + constants.MISSING_BLOCK_EXPIRE_TIME / 2)
        self.assertEqual(len(self.block_recovery_service.block_hash_to_block), 2)

        # Verify that first block is remove and the second left 2 seconds before second block expires
        self.block_recovery_service.cleanup_old_blocks(time.time() + constants.MISSING_BLOCK_EXPIRE_TIME - 2)
        self.assertEqual(len(self.block_recovery_service.block_hash_to_block), 1)

        self.assertTrue(self.block_recovery_service.cleanup_scheduled)

        # verify that the latest block left
        self.assertNotIn(self.block_hashes[0], self.block_recovery_service.block_hash_to_block)
        self.assertIn(self.block_hashes[1], self.block_recovery_service.block_hash_to_block)
        self.assertTrue(self.block_recovery_service.cleanup_scheduled)

    def _add_block(self, existing_block_count=0):
        self.block_hashes.append(os.urandom(32))

        self.blocks.append(_create_block())

        sid_base = existing_block_count * 10
        self.unknown_tx_sids.append([sid_base + 1, sid_base + 2, sid_base + 3])
        self.unknown_tx_hashes.append([os.urandom(32), os.urandom(32)])

        self.assertEqual(len(self.block_recovery_service.block_hash_to_block), existing_block_count)
        self.assertEqual(len(self.block_recovery_service.block_hash_to_sids), existing_block_count)
        self.assertEqual(len(self.block_recovery_service.block_hash_to_tx_hashes), existing_block_count)

        self.assertEqual(len(self.block_recovery_service.blocks_expiration_queue), existing_block_count)

        self.assertEqual(len(self.block_recovery_service.sid_to_block_hash), existing_block_count * 3)
        self.assertEqual(len(self.block_recovery_service.tx_hash_to_block_hash), existing_block_count * 2)

        self.block_recovery_service \
            .add_block(self.blocks[-1], self.block_hashes[-1], self.unknown_tx_sids[-1][:],
                       self.unknown_tx_hashes[-1][:])

        self.assertEqual(len(self.block_recovery_service.block_hash_to_block), existing_block_count + 1)
        self.assertEqual(len(self.block_recovery_service.block_hash_to_sids), existing_block_count + 1)
        self.assertEqual(len(self.block_recovery_service.block_hash_to_tx_hashes), existing_block_count + 1)

        self.assertEqual(len(self.block_recovery_service.blocks_expiration_queue), existing_block_count + 1)

        self.assertEqual(len(self.block_recovery_service.sid_to_block_hash), existing_block_count * 3 + 3)
        self.assertEqual(len(self.block_recovery_service.tx_hash_to_block_hash), existing_block_count * 2 + 2)

        self.assertEqual(self.block_recovery_service.block_hash_to_block[self.block_hashes[-1]], self.blocks[-1])

        self.assertEqual(self.block_recovery_service.block_hash_to_sids[self.block_hashes[-1]],
                         set(self.unknown_tx_sids[-1]))
        self.assertEqual(self.block_recovery_service.block_hash_to_tx_hashes[self.block_hashes[-1]],
                         set(self.unknown_tx_hashes[-1]))

        for sid in self.unknown_tx_sids[-1]:
            self.assertEqual(self.block_recovery_service.sid_to_block_hash[sid], self.block_hashes[-1])

        for tx_hash in self.unknown_tx_hashes[-1]:
            self.assertEqual(self.block_recovery_service.tx_hash_to_block_hash[tx_hash], self.block_hashes[-1])
