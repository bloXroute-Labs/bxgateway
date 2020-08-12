import os
import time

from mock import MagicMock

from bxcommon.test_utils.abstract_test_case import AbstractTestCase
from bxcommon.test_utils import helpers
from bxcommon.test_utils.mocks.mock_alarm_queue import MockAlarmQueue
from bxcommon.utils import crypto
from bxcommon.utils.object_hash import Sha256Hash
from bxgateway import gateway_constants

from bxgateway.services.block_recovery_service import BlockRecoveryService, RecoveredTxsSource


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
        self.bx_block_hashes = []
        self.unknown_tx_sids = []
        self.unknown_tx_hashes = []

    def test_add_block(self):
        self._add_block()
        self._add_block(1)
        self._add_block(2)

    def test_check_missing_sid(self):
        self._add_block()

        sid = self.unknown_tx_sids[0][0]

        self.block_recovery_service.check_missing_sid(sid, RecoveredTxsSource.TXS_RECEIVED_FROM_BDN)

        self.assertEqual(len(self.block_recovery_service._bx_block_hash_to_sids[self.bx_block_hashes[0]]), 2)
        self.assertNotIn(sid, self.block_recovery_service._sid_to_bx_block_hashes)

    def test_check_missing_tx_hash(self):
        self._add_block()

        tx_hash = self.unknown_tx_hashes[0][0]

        self.block_recovery_service.check_missing_tx_hash(tx_hash, RecoveredTxsSource.TXS_RECEIVED_FROM_BDN)

        self.assertEqual(len(self.block_recovery_service._bx_block_hash_to_tx_hashes[self.bx_block_hashes[0]]), 1)
        self.assertNotIn(tx_hash, self.block_recovery_service._tx_hash_to_bx_block_hashes)

    def test_cancel_recovery_for_block(self):
        self._add_block()

        self.block_recovery_service.cancel_recovery_for_block(self.block_hashes[0])
        self._assert_no_blocks_awaiting_recovery()

    def test_recovered_blocks__sids_arrive_first(self):
        self._add_block()

        # Missing sids arrive first
        for sid in self.unknown_tx_sids[0]:
            self.block_recovery_service.check_missing_sid(sid, RecoveredTxsSource.TXS_RECEIVED_FROM_BDN)

        # Then tx hashes arrive
        for tx_hash in self.unknown_tx_hashes[0]:
            self.block_recovery_service.check_missing_tx_hash(tx_hash, RecoveredTxsSource.TXS_RECEIVED_FROM_BDN)

        self._assert_no_blocks_awaiting_recovery()
        self.assertEqual(len(self.block_recovery_service.recovered_blocks), 1)
        self.assertEqual(self.block_recovery_service.recovered_blocks[0][0], self.blocks[0])
        self.assertEqual(self.block_recovery_service.recovered_blocks[0][1], RecoveredTxsSource.TXS_RECEIVED_FROM_BDN)

    def test_recovered_blocks__tx_contents_arrive_first(self):
        self._add_block()

        # Missing tx hashes arrive first
        for tx_hash in self.unknown_tx_hashes[0]:
            self.block_recovery_service.check_missing_tx_hash(tx_hash, RecoveredTxsSource.TXS_RECEIVED_FROM_BDN)

        # Then missing sids arrive
        for sid in self.unknown_tx_sids[0]:
            self.block_recovery_service.check_missing_sid(sid, RecoveredTxsSource.TXS_RECEIVED_FROM_BDN)

        self._assert_no_blocks_awaiting_recovery()
        self.assertEqual(len(self.block_recovery_service.recovered_blocks), 1)
        self.assertEqual(self.block_recovery_service.recovered_blocks[0][0], self.blocks[0])
        self.assertEqual(self.block_recovery_service.recovered_blocks[0][1], RecoveredTxsSource.TXS_RECEIVED_FROM_BDN)

    def test_clean_up_old_blocks__single_block(self):
        self.assertFalse(self.block_recovery_service._cleanup_scheduled)
        self.assertEqual(len(self.alarm_queue.alarms), 0)

        # Adding missing message
        self._add_block()

        # Verify that clean up is scheduled
        self.assertEqual(len(self.alarm_queue.alarms), 1)
        self.assertEqual(self.alarm_queue.alarms[0][0], gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME)
        self.assertEqual(self.alarm_queue.alarms[0][1], self.block_recovery_service.cleanup_old_blocks)

        self.assertTrue(self.block_recovery_service._cleanup_scheduled)

        # Run clean up before message expires and check that it is still there
        self.block_recovery_service.cleanup_old_blocks(time.time() + gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME / 2)
        self.assertEqual(len(self.block_recovery_service._bx_block_hash_to_block), 1)
        self.assertTrue(self.block_recovery_service._cleanup_scheduled)

        # Run clean up after message expires and check that it is removed
        self.block_recovery_service.cleanup_old_blocks(time.time() + gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME + 1)
        self._assert_no_blocks_awaiting_recovery()
        self.assertEqual(len(self.block_recovery_service._bx_block_hash_to_block), 0)
        self.assertFalse(self.block_recovery_service._cleanup_scheduled)

    def test_clean_up_recovered_blocks(self):
        self.assertTrue(len(self.block_recovery_service.recovered_blocks) == 0)

        # Adding ready to retry messages
        self.block_recovery_service.recovered_blocks.append((_create_block(),RecoveredTxsSource.TXS_RECEIVED_FROM_BDN))
        self.block_recovery_service.recovered_blocks.append((_create_block(), RecoveredTxsSource.TXS_RECEIVED_FROM_BDN))
        self.block_recovery_service.recovered_blocks.append((_create_block(), RecoveredTxsSource.TXS_RECEIVED_FROM_BDN))

        self.assertEqual(len(self.block_recovery_service.recovered_blocks), 3)

        # Removing ready to retry messages and verify that they are removed
        self.block_recovery_service.clean_up_recovered_blocks()

        self.assertEqual(len(self.block_recovery_service.recovered_blocks), 0)

    def test_clean_up_old_blocks__multiple_blocks(self):
        self.assertFalse(self.block_recovery_service._cleanup_scheduled)
        self.assertEqual(len(self.alarm_queue.alarms), 0)

        # Adding to blocks with 2 seconds difference between them
        self._add_block()
        time.time = MagicMock(return_value=time.time() + 3)
        self._add_block(1)

        # Verify that clean up scheduled
        self.assertEqual(len(self.alarm_queue.alarms), 1)
        self.assertEqual(self.alarm_queue.alarms[0][0], gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME)
        self.assertEqual(self.alarm_queue.alarms[0][1], self.block_recovery_service.cleanup_old_blocks)

        # Verify that both blocks are there before the first one expires
        self.block_recovery_service.cleanup_old_blocks(time.time() + gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME / 2)
        self.assertEqual(len(self.block_recovery_service._bx_block_hash_to_block), 2)

        # Verify that first block is remove and the second left 2 seconds before second block expires
        self.block_recovery_service.cleanup_old_blocks(time.time() + gateway_constants.BLOCK_RECOVERY_MAX_QUEUE_TIME - 2)
        self.assertEqual(len(self.block_recovery_service._bx_block_hash_to_block), 1)

        self.assertTrue(self.block_recovery_service._cleanup_scheduled)

        # verify that the latest block left
        self.assertNotIn(self.bx_block_hashes[0], self.block_recovery_service._bx_block_hash_to_block)
        self.assertIn(self.bx_block_hashes[1], self.block_recovery_service._bx_block_hash_to_block)
        self.assertTrue(self.block_recovery_service._cleanup_scheduled)

    def _add_block(self, existing_block_count=0):
        bx_block = _create_block()
        self.blocks.append(bx_block)
        self.block_hashes.append(Sha256Hash(os.urandom(32)))
        self.bx_block_hashes.append(Sha256Hash(crypto.double_sha256(bx_block)))

        sid_base = existing_block_count * 10
        self.unknown_tx_sids.append([sid_base + 1, sid_base + 2, sid_base + 3])
        self.unknown_tx_hashes.append([os.urandom(32), os.urandom(32)])

        self.assertEqual(existing_block_count, len(self.block_recovery_service._bx_block_hash_to_block))
        self.assertEqual(existing_block_count, len(self.block_recovery_service._bx_block_hash_to_sids))
        self.assertEqual(existing_block_count, len(self.block_recovery_service._bx_block_hash_to_tx_hashes))
        self.assertEqual(existing_block_count, len(self.block_recovery_service._bx_block_hash_to_block_hash))
        self.assertEqual(existing_block_count, len(self.block_recovery_service._blocks_expiration_queue))
        self.assertEqual(existing_block_count * 3, len(self.block_recovery_service._sid_to_bx_block_hashes))
        self.assertEqual(existing_block_count * 2, len(self.block_recovery_service._tx_hash_to_bx_block_hashes))

        self.block_recovery_service \
            .add_block(self.blocks[-1], self.block_hashes[-1], self.unknown_tx_sids[-1][:],
                       self.unknown_tx_hashes[-1][:])

        self.assertEqual(existing_block_count + 1, len(self.block_recovery_service._bx_block_hash_to_block))
        self.assertEqual(existing_block_count + 1, len(self.block_recovery_service._bx_block_hash_to_sids))
        self.assertEqual(existing_block_count + 1, len(self.block_recovery_service._bx_block_hash_to_tx_hashes))
        self.assertEqual(existing_block_count + 1, len(self.block_recovery_service._bx_block_hash_to_block_hash))
        self.assertEqual(existing_block_count + 1, len(self.block_recovery_service._blocks_expiration_queue))
        self.assertEqual(existing_block_count * 3 + 3, len(self.block_recovery_service._sid_to_bx_block_hashes))
        self.assertEqual(existing_block_count * 2 + 2, len(self.block_recovery_service._tx_hash_to_bx_block_hashes))

        self.assertEqual(self.blocks[-1], self.block_recovery_service._bx_block_hash_to_block[self.bx_block_hashes[-1]])

        self.assertEqual(set(self.unknown_tx_sids[-1]),
                         self.block_recovery_service._bx_block_hash_to_sids[self.bx_block_hashes[-1]])
        self.assertEqual(set(self.unknown_tx_hashes[-1]),
                         self.block_recovery_service._bx_block_hash_to_tx_hashes[self.bx_block_hashes[-1]])

        for sid in self.unknown_tx_sids[-1]:
            self.assertIn(self.bx_block_hashes[-1], self.block_recovery_service._sid_to_bx_block_hashes[sid])

        for tx_hash in self.unknown_tx_hashes[-1]:
            self.assertIn(self.bx_block_hashes[-1], self.block_recovery_service._tx_hash_to_bx_block_hashes[tx_hash])

    def test_multiple_compressed_versions_need_recovery(self):
        block_hash = Sha256Hash(helpers.generate_bytearray(crypto.SHA256_HASH_LEN))

        bx_block_1_v1 = helpers.generate_bytearray(100)
        bx_block_1_v1_hash = Sha256Hash(crypto.double_sha256(bx_block_1_v1))
        unknown_sids_v1 = [i for i in range(10)]
        unknown_hashes_v1 = [Sha256Hash(helpers.generate_bytearray(crypto.SHA256_HASH_LEN)) for i in range(10)]

        bx_block_1_v2 = helpers.generate_bytearray(100)
        bx_block_1_v2_hash = Sha256Hash(crypto.double_sha256(bx_block_1_v2))
        unknown_sids_v2 = [i for i in range(5, 15)]
        unknown_hashes_v2 = unknown_hashes_v1[5:]
        unknown_hashes_v2.extend(Sha256Hash(helpers.generate_bytearray(crypto.SHA256_HASH_LEN)) for i in range(5))

        self.block_recovery_service.add_block(bx_block_1_v1, block_hash, unknown_sids_v1, unknown_hashes_v1)
        self.assertEqual(1, len(self.block_recovery_service._bx_block_hash_to_sids))
        self.assertEqual(10, len(self.block_recovery_service._bx_block_hash_to_sids[bx_block_1_v1_hash]))
        self.assertEqual(1, len(self.block_recovery_service._bx_block_hash_to_tx_hashes))
        self.assertEqual(10, len(self.block_recovery_service._bx_block_hash_to_tx_hashes[bx_block_1_v1_hash]))
        self.assertEqual(1, len(self.block_recovery_service._bx_block_hash_to_block))
        self.assertEqual(10, len(self.block_recovery_service._sid_to_bx_block_hashes))
        self.assertEqual(10, len(self.block_recovery_service._tx_hash_to_bx_block_hashes))

        self.block_recovery_service.check_missing_sid(unknown_sids_v1[0], RecoveredTxsSource.TXS_RECEIVED_FROM_BDN)
        self.block_recovery_service.check_missing_sid(unknown_sids_v1[1], RecoveredTxsSource.TXS_RECEIVED_FROM_BDN)
        self.block_recovery_service.check_missing_tx_hash(unknown_hashes_v1[0], RecoveredTxsSource.TXS_RECEIVED_FROM_BDN)

        self.assertEqual(8, len(self.block_recovery_service._bx_block_hash_to_sids[bx_block_1_v1_hash]))
        self.assertEqual(9, len(self.block_recovery_service._bx_block_hash_to_tx_hashes[bx_block_1_v1_hash]))
        self.assertEqual(8, len(self.block_recovery_service._sid_to_bx_block_hashes))
        self.assertEqual(9, len(self.block_recovery_service._tx_hash_to_bx_block_hashes))
        self.assertEqual(0, len(self.block_recovery_service.recovered_blocks))

        self.block_recovery_service.add_block(bx_block_1_v2, block_hash, unknown_sids_v2, unknown_hashes_v2)

        self.assertEqual(10, len(self.block_recovery_service._bx_block_hash_to_sids[bx_block_1_v2_hash]))
        self.assertEqual(10, len(self.block_recovery_service._bx_block_hash_to_tx_hashes[bx_block_1_v2_hash]))
        self.assertEqual(13, len(self.block_recovery_service._sid_to_bx_block_hashes))
        self.assertEqual(14, len(self.block_recovery_service._tx_hash_to_bx_block_hashes))
        self.assertEqual(0, len(self.block_recovery_service.recovered_blocks))

        for unknown_sid in unknown_sids_v2:
            self.block_recovery_service.check_missing_sid(unknown_sid, RecoveredTxsSource.TXS_RECEIVED_FROM_BDN)
        for unknown_hash in unknown_hashes_v2:
            self.block_recovery_service.check_missing_tx_hash(unknown_hash, RecoveredTxsSource.TXS_RECEIVED_FROM_BDN)

        # recovering one block should cancel recovery for the second
        self.assertNotIn(block_hash, self.block_recovery_service._bx_block_hash_to_sids)
        self.assertNotIn(block_hash, self.block_recovery_service._bx_block_hash_to_tx_hashes)
        self.assertEqual(0, len(self.block_recovery_service._sid_to_bx_block_hashes))
        self.assertEqual(0, len(self.block_recovery_service._tx_hash_to_bx_block_hashes))
        self.assertEqual(1, len(self.block_recovery_service.recovered_blocks))

        self._assert_no_blocks_awaiting_recovery()

    def _assert_no_blocks_awaiting_recovery(self):
        self.assertEqual(0, len(self.block_recovery_service._bx_block_hash_to_block))
        self.assertEqual(0, len(self.block_recovery_service._bx_block_hash_to_sids))
        self.assertEqual(0, len(self.block_recovery_service._bx_block_hash_to_tx_hashes))
        self.assertEqual(0, len(self.block_recovery_service._bx_block_hash_to_block_hash))

        self.assertEqual(0, len(self.block_recovery_service._sid_to_bx_block_hashes))
        self.assertEqual(0, len(self.block_recovery_service._tx_hash_to_bx_block_hashes))
        self.assertEqual(0, len(self.block_recovery_service._block_hash_to_bx_block_hashes))

