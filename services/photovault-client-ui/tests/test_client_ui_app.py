from photovault_client_ui.app import create_app


def test_index_route() -> None:
    app = create_app()
    client = app.test_client()
    response = client.get("/")
    assert response.status_code == 200
