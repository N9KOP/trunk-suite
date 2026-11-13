package trunking

import (
	"testing"
	"time"

	"github.com/MattCheramie/GopherTrunk/internal/events"
)

func mkDataEngine(t *testing.T, ignore map[string]bool) (*Engine, *events.Bus) {
	t.Helper()
	bus := events.NewBus(16)
	pool, _ := mkPool(1)
	clock := &fakeClock{t: time.Unix(1000, 0)}
	e, err := NewEngine(EngineOptions{
		Bus:             bus,
		VoicePool:       pool,
		Talkgroups:      NewTalkgroupDB(),
		CallTimeout:     time.Hour,
		Now:             clock.Now,
		IgnoreDataCalls: ignore,
	})
	if err != nil {
		t.Fatal(err)
	}
	return e, bus
}

func TestEngineIgnoreDataCallsDropsDataGrant(t *testing.T) {
	e, bus := mkDataEngine(t, map[string]bool{"X": true})
	defer bus.Close()

	e.HandleGrant(Grant{System: "X", Protocol: "p25", GroupID: 100, FrequencyHz: 851_000_000, DataCall: true})

	if got := e.ActiveCalls(); len(got) != 0 {
		t.Fatalf("ignore_data_calls must not allocate a voice SDR to a data grant; active = %+v", got)
	}
}

func TestEngineIgnoreDataCallsStillFollowsVoice(t *testing.T) {
	e, bus := mkDataEngine(t, map[string]bool{"X": true})
	defer bus.Close()

	e.HandleGrant(Grant{System: "X", Protocol: "p25", GroupID: 100, FrequencyHz: 851_000_000})

	if got := e.ActiveCalls(); len(got) != 1 {
		t.Fatalf("voice grants must still be followed; active = %+v", got)
	}
}

func TestEngineIgnoreDataCallsLetsEmergencyThrough(t *testing.T) {
	e, bus := mkDataEngine(t, map[string]bool{"X": true})
	defer bus.Close()

	e.HandleGrant(Grant{System: "X", Protocol: "p25", GroupID: 100, FrequencyHz: 851_000_000, DataCall: true, Emergency: true})

	if got := e.ActiveCalls(); len(got) != 1 {
		t.Fatalf("emergency data grants bypass the policy; active = %+v", got)
	}
}

func TestEngineDataCallsFollowedByDefault(t *testing.T) {
	e, bus := mkDataEngine(t, nil)
	defer bus.Close()

	e.HandleGrant(Grant{System: "X", Protocol: "p25", GroupID: 100, FrequencyHz: 851_000_000, DataCall: true})

	if got := e.ActiveCalls(); len(got) != 1 {
		t.Fatalf("a system without ignore_data_calls follows data grants; active = %+v", got)
	}
}
