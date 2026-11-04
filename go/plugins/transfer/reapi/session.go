package reapi

import (
	"context"
	"errors"
	"github.com/wignerStan/network-data-plane-worker/go/contracts/plugin/encoded"
)

type session struct {
	generation, next    uint64
	shape               encoded.Shape
	maxBytes            int64
	pending             map[uint64]encoded.Action
	started, closed     bool
	requests, responses uint64
	deferred            *encoded.Event
}

func (s *session) action(kind encoded.Kind, side encoded.Side) encoded.Action {
	s.next++
	a := encoded.Action{ID: s.next, Kind: kind, Side: side}
	s.pending[a.ID] = a
	return a
}
func (s *session) finish(e encoded.Event) []encoded.Action {
	a := s.action(encoded.Finish, 0)
	a.Result = e.Result
	a.Code = e.Code
	s.closed = true
	return []encoded.Action{a}
}
func (s *session) Step(ctx context.Context, e encoded.Event) (actions []encoded.Action, failure error) {
	defer func() {
		if failure != nil {
			_ = s.Close()
		}
	}()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if s.closed {
		return nil, errors.New("closed transfer")
	}
	if s.next > ^uint64(0)-encoded.MaxActions {
		return nil, errors.New("action identity exhausted")
	}
	if !s.started {
		if e.Kind != encoded.Begin || e.ID != 0 {
			return nil, errors.New("expected begin")
		}
		s.started = true
		a := s.action(encoded.Open, encoded.Upstream)
		a.Shape = s.shape
		s.pending[a.ID] = a
		return []encoded.Action{a}, nil
	}
	expected, ok := s.pending[e.ID]
	if !ok || e.Kind != expected.Kind || e.Side != expected.Side {
		return nil, errors.New("unexpected action completion")
	}
	delete(s.pending, e.ID)
	if e.Code != 0 && !e.EOF && e.Kind != encoded.Receive {
		return s.finish(e), nil
	}
	switch e.Kind {
	case encoded.Open:
		return []encoded.Action{s.action(encoded.Receive, encoded.Downstream), s.action(encoded.Receive, encoded.Upstream)}, nil
	case encoded.Headers:
		if s.deferred == nil {
			return nil, errors.New("missing header continuation")
		}
		event := *s.deferred
		s.deferred = nil
		return s.received(event)
	case encoded.Receive:
		if e.Headers != 0 {
			copy := e
			copy.Headers = 0
			s.deferred = &copy
			a := s.action(encoded.Headers, 0)
			a.Result = e.Headers
			s.pending[a.ID] = a
			return []encoded.Action{a}, nil
		}
		return s.received(e)
	case encoded.Send:
		a := s.action(encoded.Release, expected.Side)
		a.Frame = expected.Frame
		s.pending[a.ID] = a
		return []encoded.Action{a}, nil
	case encoded.Release:
		source := encoded.Upstream
		if expected.Side == encoded.Upstream {
			source = encoded.Downstream
		}
		return []encoded.Action{s.action(encoded.Receive, source)}, nil
	case encoded.HalfClose:
		return nil, nil
	default:
		return nil, errors.New("unexpected transfer event")
	}
}
func (s *session) received(e encoded.Event) ([]encoded.Action, error) {
	if e.EOF {
		if e.Side == encoded.Downstream {
			return []encoded.Action{s.action(encoded.HalfClose, encoded.Upstream)}, nil
		}
		return s.finish(e), nil
	}
	if e.Code != 0 {
		return s.finish(e), nil
	}
	if e.Frame.ID == 0 || e.Frame.Generation != s.generation || e.Frame.Bytes < 0 || e.Frame.Bytes > s.maxBytes {
		return nil, errors.New("invalid frame lease")
	}
	target := encoded.Downstream
	if e.Side == encoded.Downstream {
		s.requests++
		if !s.shape.ClientStreaming && s.requests > 1 {
			return nil, &encoded.Failure{Code: 3, Message: "too many request messages"}
		}
		target = encoded.Upstream
	} else {
		s.responses++
		if !s.shape.ServerStreaming && s.responses > 1 {
			return nil, &encoded.Failure{Code: 13, Message: "too many response messages"}
		}
	}
	a := s.action(encoded.Send, target)
	a.Frame = e.Frame
	s.pending[a.ID] = a
	return []encoded.Action{a}, nil
}
func (s *session) Close() error { s.closed = true; clear(s.pending); s.deferred = nil; return nil }
