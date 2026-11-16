package main

import (
	"context"
	"strconv"
	"strings"
	"time"
)

// ---- request kinds (quota-model 50 §2.9, C4) ----
//
// The router serves requests of more than one kind. Binding and the pointer,
// the dial check, the overlay and its records, the walk, health evidence and
// the unavailable reply are shared; what differs per kind is only which
// members can take the request at all and how one member's upstream request
// is built. That is the adapter below — the chat adapter is the code the
// router always ran, moved; the image adapter hands the build to the member's
// wire, so a second image provider is a new wire with its own request and
// reply shapes (Runway, C8) and no branch here.

// classReq is one class-routed request, as the router carries it from the
// bind to the last rebuild: a chat request (the pre-rewrite Anthropic body,
// model = the class picker id, so a cross-member rewrite is always one clean
// splice; the INBOUND path, since the outbound one already embeds attempt 1's
// upstream base path — kimi serves under /coding; and whether it arrived on
// the OpenAI wire), or an image request, which is ONE image of the door's
// batch (the door makes one checked dial per image, §2.2 row 19).
type classReq struct {
	origBody   []byte
	inPath     string
	wireOpenAI bool         // #96: arrived on the OpenAI wire; an image request always replies there
	image      imageRequest // non-nil: an image request (an edition's image door)
	// ctx is the request's own (nil: a caller with no client, such as the
	// status page). What the routing asks on the request's behalf, such as a
	// dispensed key (apikey.go), ends with it.
	ctx context.Context
}

// context is the request's context, or Background for a caller with none.
func (q classReq) context() context.Context {
	if q.ctx == nil {
		return context.Background()
	}
	return q.ctx
}

// cantServe is why a member can never take a request (§2.9, capability
// before money): the request field at fault, in the Images API's own name
// ("size", "mask"; "" where no one field is), the reason in words, what the
// member accepts for that field, and for a size the member's nearest sizes,
// nearest first and priced. It goes in the unavailable reply as the member's
// cannot_serve, and its text in the log line of the skip.
type cantServe struct {
	Field   string        `json:"field,omitempty"`
	Reason  string        `json:"reason"`
	Accepts string        `json:"accepts,omitempty"`
	Nearest []nearestSize `json:"nearest,omitempty"`
}

// nearestSize is one size a member draws near a size it refused, at its price
// for the request's quality where the upstream states one in advance.
type nearestSize struct {
	Size    string   `json:"size"`
	Price   *float64 `json:"price,omitempty"`
	Unit    string   `json:"unit,omitempty"`
	Quality string   `json:"quality,omitempty"`
	// DrawsNow is whether the member would draw the request at this size
	// now, its check for all the request's images: set in a 400's body
	// (hpImagesRefusalRoutes), absent elsewhere.
	DrawsNow *bool `json:"draws_now,omitempty"`
}

// String is the refusal as one line: the field, then the reason.
func (c *cantServe) String() string {
	if c == nil {
		return ""
	}
	if c.Field == "" {
		return c.Reason
	}
	return c.Field + ": " + c.Reason
}

// requestKind is the per-kind adapter.
type requestKind interface {
	// name is the kind's word in a refusal: "chat" or "image".
	name() string
	// canServe reports why member m can never take q (nil = it can), whatever
	// its quota or pay: capability before money (§2.9). It is a property of
	// the member and the request, never of the accounts.
	canServe(m classMemberPref, q classReq) *cantServe
	// prepare fills provider member m's outbound request facts on info (body,
	// and a translated wire's per-request credential) for q. A non-empty
	// return is a refusal: the try list moves on.
	prepare(p *provider, m classMemberPref, info *reqInfo, q classReq) string
	// request is what member m's dial for q is checked for (§1.6): the most
	// it can charge and how many outputs it counts. Zero where the cost is
	// learned afterwards (every chat request, a codex image).
	request(m classMemberPref, q classReq) checkRequest
}

func (q classReq) kind() requestKind {
	if q.image != nil {
		return q.image.kind()
	}
	return chatKind{}
}

// imageRequest is one image of an image door's batch: the door (an edition's)
// defines the request and its kind adapter.
type imageRequest interface {
	kind() requestKind
	// refusalRoutes completes the routes of a refusal of the request (the
	// door's 400, classRefusal) with what the door adds to them.
	refusalRoutes(members []classMemberPref, q classReq, routes []unavailRoute, now time.Time)
}

// chatKind is the router's code before C4, moved: D2's skip and the member's
// wire preparing the spliced body.
type chatKind struct{}

func (chatKind) name() string { return "chat" }

func (chatKind) canServe(m classMemberPref, q classReq) *cantServe {
	if q.wireOpenAI && m.Kind == "anthropic" {
		// D2: a subscription bearer is never put behind an OpenAI-compatible
		// endpoint.
		return &cantServe{Reason: "an anthropic member is never dialled from the openai wire"}
	}
	return nil
}

func (chatKind) request(classMemberPref, classReq) checkRequest { return checkRequest{} }

func (chatKind) prepare(p *provider, m classMemberPref, info *reqInfo, q classReq) string {
	body := q.origBody
	if b := rewriteModel(q.origBody, m.Model); b != nil {
		body = b
	}
	return p.wire.prepare(info, body, q.inPath)
}

// plainCreditUnits are the price units that read as plain "credits" (an
// edition's provider registers its own).
var plainCreditUnits = map[string]bool{}

// nearestWords is a list of nearest sizes in words, one image's price each:
// "1088x1920 (20 credits at high), 1440x2560 (41 credits at high)".
func nearestWords(ns []nearestSize) string { return nearestWordsFor(ns, 1) }

// nearestWordsFor is nearestWords for a request of n images: past one, each
// price is per image, with the request's total beside it: "1088x1920 (20
// credits each at high, 40 for 2)".
func nearestWordsFor(ns []nearestSize, n int) string {
	parts := make([]string, 0, len(ns))
	for _, s := range ns {
		w := s.Size
		if s.Price != nil {
			unit := "credits"
			if s.Unit != "" && !plainCreditUnits[s.Unit] {
				unit = s.Unit
			}
			w += " (" + fmtNum(*s.Price) + " " + unit
			if n > 1 {
				w += " each"
			}
			if s.Quality != "" {
				w += " at " + s.Quality
			}
			if n > 1 {
				w += ", " + fmtNum(*s.Price*float64(n)) + " for " + strconv.Itoa(n)
			}
			w += ")"
		}
		parts = append(parts, w)
	}
	return strings.Join(parts, ", ")
}

func fmtNum(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }
