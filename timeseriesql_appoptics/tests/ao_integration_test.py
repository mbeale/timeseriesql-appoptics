import unittest
import os
import time
import random
import string
from timeseriesql_appoptics import AOBackend, create_scalar_time_series


PERFORM_INTEGRATION_TESTS = os.environ.get("AO_INTEGRATIONS_TESTING", False)
APPOPTICS_TOKEN = os.environ.get("APPOPTICS_TOKEN", False)


def is_ao_response_equal(expected, actual):
    pass


def randString():
    """Generate a random string of fixed length """
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for i in range(20))


@unittest.skipUnless(PERFORM_INTEGRATION_TESTS, "skipping integrations tests")
@unittest.skipUnless(APPOPTICS_TOKEN, "APPOPTICS_TOKEN is not set")
class TestIntegrationAOBackend(unittest.TestCase):
    def setUp(self):
        # setup start_time
        start_time = int(time.time())
        start_time = start_time - (start_time % 60) - 3600
        self.start_time = start_time

        # setup metrics and tagsets
        self.metric1_name = randString()
        self.metric2_name = randString()
        self.metric3_name = randString()
        self.tag_set1 = {"host": randString(), "env": "prod", "service": "service1"}
        self.tag_set2 = {"host": randString(), "env": "prod", "service": "service2"}
        self.tag_set3 = {"host": randString(), "env": "test", "service": "service1"}

        # back fill metrics
        measurements = []
        for i in range(60):
            for metric_index in range(1, 4):
                for tag_set_index in range(1, 4):
                    measurements.append(
                        {
                            "name": getattr(self, f"metric{metric_index}_name"),
                            "tags": getattr(self, f"tag_set{tag_set_index}"),
                            "value": (tag_set_index + (tag_set_index * i)),
                            "time": self.start_time + (i * 60),
                        }
                    )

        AOBackend().post("measurements", {"measurements": measurements})
        time.sleep(5)  # need a delay for the measurements to fill in the system

    def tearDown(self):
        AOBackend().delete(
            "metrics",
            {"names": [self.metric1_name, self.metric2_name, self.metric3_name]},
        )

    def test_basic_query(self):
        data = AOBackend(x for x in self.metric1_name).range(
            start=self.start_time, resolution=60
        )
        # verify the shape
        self.assertEqual(data.shape, (60, 3))
        # check tags
        tags_in_labels = [l["host"] for l in data.labels]
        tags_in_labels.sort()
        expected_tags = [
            self.tag_set1["host"],
            self.tag_set2["host"],
            self.tag_set3["host"],
        ]
        expected_tags.sort()
        self.assertEqual(tags_in_labels, expected_tags)
        # check a data point
        self.assertEqual(data[{"host": self.tag_set2["host"]}][1, 0], 4.0)


if __name__ == "__main__":
    unittest.main()
