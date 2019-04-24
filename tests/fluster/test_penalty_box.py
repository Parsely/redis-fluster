import time
import unittest

import mock
from redis.exceptions import ConnectionError

from fluster.penalty_box import PenaltyBox


class PenaltyBoxTests(unittest.TestCase):
    def setUp(self):
        self.box = PenaltyBox(min_wait=0.5, max_wait=2)

    def _raise_exc(self, times, retval=None):
        """Function which raises ConnectionError `times` times."""
        # FIXME: Scoping so `raised` isn't a class var
        self.raised = 0

        def raiser(*args, **kwargs):
            if self.raised < times:
                self.raised += 1
                raise ConnectionError()
            return retval

        return raiser

    def test_penalty_box(self):
        """Basic test that the penalty box works."""
        client = mock.MagicMock()
        client.pool_id = "foo"
        client.echo.side_effect = self._raise_exc(0)
        self.box.add(client)
        self.assertEqual(list(self.box.get()), [])
        time.sleep(1)
        self.assertEqual(list(self.box.get()), [client])

    def test_penalty_box_backoff(self):
        """Basic test that the penalty box backs off."""
        client = mock.MagicMock()
        client.pool_id = "foo"
        client.echo.side_effect = self._raise_exc(1)
        self.box.add(client)
        self.assertEqual(list(self.box.get()), [])
        time.sleep(1)
        self.assertEqual(list(self.box.get()), [])
        time.sleep(1)
        self.assertEqual(list(self.box.get()), [client])


if __name__ == "__main__":
    unittest.main()
