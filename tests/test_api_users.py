from flask import current_app

from granite.api import db
from granite.api.models import User


def test_add_user(test_app, test_database, add_user):
    user = add_user("test1@test.com", "test")
    assert user.id
    assert user.email == "test1@test.com"
    assert user.active
    assert user.password


def test_passwords_are_random(test_app, test_database, add_user):
    user_one = add_user("test@test.com", "greaterthaneight")
    user_two = add_user("test@test2.com", "greaterthaneight")
    assert user_one.password != user_two.password


def test_encode_auth_token(test_app, test_database, add_user):
    user = add_user("test@test3.com", "test")
    auth_token = user.encode_auth_token(user.id)
    assert isinstance(auth_token, str)
    assert User.decode_auth_token(auth_token) == user.id


def test_user_registration(client, test_database, add_user):
    response = client.post("/api/v1/register", json={"email": "test4@test.com", "password": "123456"})
    data = response.get_json()

    assert data["status"] == "success"
    assert data["message"] == "test4@test.com was added"
    assert data["auth_token"]
    assert response.content_type == "application/json"
    assert response.status_code == 200


def test_user_registration_duplicate_email(client, test_database, add_user):
    add_user("test5@test.com", "test")
    response = client.post("/api/v1/register", json={"email": "test5@test.com", "password": "test"})
    data = response.get_json()
    response.status_code == 400
    assert "Sorry. That email already exists." in data["message"]
    assert "failure" in data["status"]


def test_user_registration_invalid_json(client, test_database):
    response = client.post("/api/v1/register", json={})
    data = response.get_json()
    assert response.status_code == 400
    assert "Invalid payload." in data["message"]
    assert "failure" in data["status"]


def test_user_registration_invalid_json_keys_no_email(client, test_database):
    response = client.post("/api/v1/register", json={"password": "test1234"})
    data = response.get_json()
    assert response.status_code == 400
    assert "Invalid payload." in data["message"]
    assert "failure" in data["status"]


def test_user_registration_invalid_json_keys_no_password(client, test_database):
    response = client.post("/api/v1/register", json={"email": "test6@test.com"})
    data = response.get_json()
    assert response.status_code == 400
    assert "Invalid payload." in data["message"]
    assert "failure" in data["status"]


def test_registered_user_login(client, test_database, add_user):
    add_user("test6@test.com", "test")
    response = client.post("/api/v1/login", json={"email": "test6@test.com", "password": "test"})
    data = response.get_json()
    assert data["status"] == "success"
    assert data["message"] == "Successfully logged in."
    assert data["auth_token"]
    assert response.content_type == "application/json"
    assert response.status_code, 200


def test_not_registered_user_login(client, test_database, add_user):
    response = client.post("/api/v1/login", json={"email": "dnetest@test.com", "password": "test"})
    data = response.get_json()
    assert data["status"] == "failure"
    assert data["message"] == "User does not exist."
    assert response.content_type == "application/json"
    assert response.status_code, 404


def test_valid_logout(client, test_database, add_user):
    add_user("test7@test.com", "test")

    # Valid token logout
    response = client.post("/api/v1/login", json={"email": "test7@test.com", "password": "test"})
    data = response.get_json()
    token = data["auth_token"]

    response = client.get("/api/v1/logout", headers={"Authorization": f"Bearer {token}"})
    data = response.get_json()
    assert data["status"] == "success"
    assert data["message"] == "Successfully logged out."
    assert response.status_code == 200


def test_invalid_logout_expired_token(client, test_database, add_user):
    add_user("test8@test.com", "test")
    current_app.config["TOKEN_EXPIRATION_SECONDS"] = -1
    resp_login = client.post("/api/v1/login", json={"email": "test8@test.com", "password": "test"})
    # invalid token logout
    token = resp_login.get_json()["auth_token"]

    response = client.get("/api/v1/logout", headers={"Authorization": f"Bearer {token}"})
    data = response.get_json()
    assert data["status"] == "failure"
    assert data["message"] == "Signature expired. Please log in again."
    assert response.status_code == 401


def test_invalid_logout(client, test_database):
    response = client.get("/api/v1/logout", headers={"Authorization": "Bearer invalid"})
    data = response.get_json()
    assert data["status"] == "failure"
    assert data["message"] == "Invalid token. Please log in again."
    assert response.status_code == 401


def test_invalid_logout_inactive(client, test_database, add_user):
    add_user("test9@test.com", "test")
    current_app.config["TOKEN_EXPIRATION_SECONDS"] = 5
    # update user
    user = User.query.filter_by(email="test9@test.com").first()
    user.active = False
    db.session.commit()

    resp_login = client.post("/api/v1/login", json={"email": "test9@test.com", "password": "test"})
    token = resp_login.get_json()["auth_token"]
    response = client.get("/api/v1/logout", headers={"Authorization": f"Bearer {token}"})
    data = response.get_json()
    assert data["status"] == "failure"
    assert data["message"] == "Provide a valid auth token."
    assert response.status_code == 401
