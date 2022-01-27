import pytest


@pytest.mark.mmm
def test_access_grants_exist(connection):
    model = connection.get_model("test_model")

    assert len(model.access_grants) == 3
    assert model.access_grants[0]["name"] == "test_access_grant_department_explore"
    assert model.access_grants[0]["user_attribute"] == "department"
    assert model.access_grants[0]["allowed_values"] == ["finance", "executive", "marketing"]
