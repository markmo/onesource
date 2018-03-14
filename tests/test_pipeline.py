from pipeline import AbstractStep, Pipeline
import pytest


# Fake Exception to test successful run - do not use Exception
# which will be caught by Pipeline as an abnormal end
class Success(BaseException):
    pass


# TODO
# I could achieve the above with mock


def test_pipeline_add_and_run_step():
    class TestStep(AbstractStep):
        def run(self, ctrl, log, acc):
            raise Success

    pipeline = Pipeline({})
    pipeline.add_step(TestStep('Test Step'))

    with pytest.raises(Success):
        pipeline.run()


def test_pipeline_add_and_run_multiple_steps():
    class TestStep1(AbstractStep):
        def run(self, ctrl, log, acc):
            pass

    class TestStep2(AbstractStep):
        def run(self, ctrl, log, acc):
            raise Success

    pipeline = Pipeline({})
    pipeline.add_steps([
        TestStep1('Test Step 1'),
        TestStep2('Test Step 2')
    ])

    with pytest.raises(Success):
        pipeline.run()
