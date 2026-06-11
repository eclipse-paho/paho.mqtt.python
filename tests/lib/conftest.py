import os
import signal
import subprocess
import sys

import pytest

from tests.consts import ssl_path, tests_path
from tests.paho_test import create_server_socket, create_server_socket_ssl, ssl

clients_path = tests_path / "lib" / "clients"


def _yield_server(monkeypatch, sockport):
    sock, port = sockport
    monkeypatch.setenv("PAHO_SERVER_PORT", str(port))
    try:
        yield sock
    finally:
        sock.close()


@pytest.fixture()
def server_socket(monkeypatch):
    yield from _yield_server(monkeypatch, create_server_socket())


@pytest.fixture()
def ssl_server_socket(monkeypatch):
    if ssl is None:
        pytest.skip("no ssl module")
    yield from _yield_server(monkeypatch, create_server_socket_ssl())


@pytest.fixture()
def alpn_ssl_server_socket(monkeypatch):
    if ssl is None:
        pytest.skip("no ssl module")
    if not getattr(ssl, "HAS_ALPN", False):
        pytest.skip("ALPN not supported in this version of Python")
    yield from _yield_server(monkeypatch, create_server_socket_ssl(alpn_protocols=["paho-test-protocol"]))


def stop_process(proc: subprocess.Popen) -> None:
    if sys.platform == "win32":
        proc.send_signal(signal.CTRL_C_EVENT)
    else:
        proc.send_signal(signal.SIGINT)
    try:
        proc.wait(5)
    except subprocess.TimeoutExpired:
        proc.terminate()


@pytest.fixture()
def start_client(request: pytest.FixtureRequest):
    def starter(name: str, expected_returncode: int = 0) -> None:
        client_path = clients_path / name
        if not client_path.exists():
            raise FileNotFoundError(client_path)
        env = dict(
            os.environ,
            PAHO_SSL_PATH=str(ssl_path),
            PYTHONPATH=f"{tests_path}{os.pathsep}{os.environ.get('PYTHONPATH', '')}",
        )
        assert 'PAHO_SERVER_PORT' in env, "PAHO_SERVER_PORT must be set in the environment when starting a client"
        proc = subprocess.Popen([  # noqa: S603
            sys.executable,
            str(client_path),
        ], env=env)

        def fin():
            stop_process(proc)
            # Return code can be None on Python 3.8
            # https://docs.python.org/library/subprocess.html#subprocess.Popen.returncode
            if proc.returncode is not None and proc.returncode != expected_returncode:
                raise RuntimeError(f"Client {name} exited with code {proc.returncode}, expected {expected_returncode}")

        request.addfinalizer(fin)
        return proc

    return starter
