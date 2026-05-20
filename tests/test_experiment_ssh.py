import unittest
from unittest.mock import AsyncMock, patch

import asyncssh

from cli.experiment import NodeInfo, _connect_node


class ExperimentSshTests(unittest.IsolatedAsyncioTestCase):
    async def test_connect_node_retries_until_success(self) -> None:
        node = NodeInfo(
            id="N1",
            host="203.0.113.10",
            user="ubuntu",
            node_type="compute",
            address="10.20.0.10",
        )
        connection = object()

        with patch(
            "cli.experiment.asyncssh.connect",
            new=AsyncMock(side_effect=[TimeoutError("slow boot"), connection]),
        ) as connect_mock, patch(
            "cli.experiment.asyncio.sleep",
            new=AsyncMock(),
        ) as sleep_mock:
            result = await _connect_node(node, "/tmp/key", None, retries=2, retry_delay=1)

        self.assertIs(result, connection)
        self.assertEqual(connect_mock.await_count, 2)
        sleep_mock.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
