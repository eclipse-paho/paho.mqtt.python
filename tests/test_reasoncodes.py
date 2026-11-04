import pytest

from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode, ReasonCodes


class TestReasonCode:
    def test_equality(self):
        rc_success = ReasonCode(PacketTypes.CONNACK, "Success")
        assert rc_success == 0
        assert rc_success == "Success"
        assert rc_success != "Protocol error"
        assert rc_success == ReasonCode(PacketTypes.CONNACK, "Success")

        rc_protocol_error = ReasonCode(PacketTypes.CONNACK, "Protocol error")
        assert rc_protocol_error == 130
        assert rc_protocol_error == "Protocol error"
        assert rc_protocol_error != "Success"
        assert rc_protocol_error == ReasonCode(PacketTypes.CONNACK, "Protocol error")

    def test_comparison(self):
        rc_success = ReasonCode(PacketTypes.CONNACK, "Success")
        rc_protocol_error = ReasonCode(PacketTypes.CONNACK, "Protocol error")

        assert not rc_success > 0
        assert rc_protocol_error > 0
        assert rc_success == 0
        assert rc_protocol_error != 0

    def test_compatibility(self):
        rc_success = ReasonCode(PacketTypes.CONNACK, "Success")
        with pytest.deprecated_call():
            rc_success_old = ReasonCodes(PacketTypes.CONNACK, "Success")
        assert rc_success == rc_success_old

        assert isinstance(rc_success, ReasonCode)
        assert isinstance(rc_success_old, ReasonCodes)
        # User might use isinstance with the old name (plural)
        # while the library give them a ReasonCode (singular) in the callbacks
        assert isinstance(rc_success, ReasonCodes)
        # The other way around is probably never used... but still support it
        assert isinstance(rc_success_old, ReasonCode)

        # Check that isinstance implementation don't always return True
        assert not isinstance(rc_success, dict)
        assert not isinstance(rc_success_old, dict)
        assert not isinstance({}, ReasonCode)
        assert not isinstance({}, ReasonCodes)
