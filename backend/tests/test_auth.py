import json
import time
import unittest

from app.auth import DEFAULT_TOKEN_TTL_SECONDS, _b64decode, create_token


class AuthTokenTests(unittest.TestCase):
    def test_default_token_ttl_is_fifteen_days(self) -> None:
        before = int(time.time())
        token = create_token("user@example.com")
        payload_b64, _ = token.split(".", 1)
        payload = json.loads(_b64decode(payload_b64))

        self.assertEqual(DEFAULT_TOKEN_TTL_SECONDS, 60 * 60 * 24 * 15)
        self.assertGreaterEqual(payload["exp"] - before, DEFAULT_TOKEN_TTL_SECONDS - 1)
        self.assertLessEqual(payload["exp"] - before, DEFAULT_TOKEN_TTL_SECONDS + 1)


if __name__ == "__main__":
    unittest.main()
