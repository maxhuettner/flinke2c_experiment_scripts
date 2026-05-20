import tempfile
import textwrap
import unittest
from pathlib import Path

from cli.experiment import load_experiments


class ExperimentConfigTests(unittest.TestCase):
    def test_top_level_num_task_slots_becomes_default(self) -> None:
        content = textwrap.dedent(
            """
            repetitions: 5
            num_task_slots: 1
            bid_src_extra_arg: --num-events 920000
            max_query_runtime: 30000

            experiments:
              - name: q1_default
                system: flink
                query: q1
              - name: q2_override
                system: flink
                query: q2
                num_task_slots: 3
                bid_src_extra_arg: --num-events 5000
                max_query_runtime: 45000
            """
        ).strip()

        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "experiments.yml"
            path.write_text(content)

            experiments = load_experiments(str(path))

        self.assertEqual(experiments[0].num_task_slots, 1)
        self.assertEqual(experiments[0].bid_src_extra_arg, "--num-events 920000")
        self.assertEqual(experiments[0].max_query_runtime, 30000)
        self.assertEqual(experiments[1].num_task_slots, 3)
        self.assertEqual(experiments[1].bid_src_extra_arg, "--num-events 5000")
        self.assertEqual(experiments[1].max_query_runtime, 45000)


if __name__ == "__main__":
    unittest.main()
