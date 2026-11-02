from airflow.models import DagBag

EXPECTED_TASKS = {
    "resolve_date",
    "load_dim_city",
    "extract_weather",
    "validate_raw",
    "transform_to_silver",
    "quality_gate",
    "load_silver",
    "build_gold",
}


def _dag():
    bag = DagBag(dag_folder="/opt/airflow/dags", include_examples=False)
    assert bag.import_errors == {}, bag.import_errors
    dag = bag.get_dag("weather_pipeline")
    assert dag is not None
    return dag


def test_dag_loads_without_errors_and_has_expected_tasks():
    assert {t.task_id for t in _dag().tasks} == EXPECTED_TASKS


def test_every_task_has_retries_and_failure_alert():
    for task in _dag().tasks:
        assert task.retries >= 3, task.task_id
        assert task.on_failure_callback is not None, task.task_id


def test_quality_gate_sits_between_transform_and_load():
    dag = _dag()
    assert "quality_gate" in dag.get_task("transform_to_silver").downstream_task_ids
    assert "load_silver" in dag.get_task("quality_gate").downstream_task_ids
    assert "build_gold" in dag.get_task("load_silver").downstream_task_ids
    assert "load_dim_city" in dag.get_task("load_silver").upstream_task_ids
