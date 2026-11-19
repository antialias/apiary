# Privacy

*For members of the hive at apiary.wtf. Plain words, no fine print. Last changed 2026-11-19.*

Apiary sits between your coding agent and your AI accounts. Everything your
agent sends flies through it. So you deserve a straight answer to two
questions: what does Apiary see, and what does it keep?

## The short answer

**Your words fly through. Apiary only logs the trip.**

Your prompts, the replies, your code and your file names go to your AI
provider and come back. Apiary passes them along and forgets them. It never
writes them down, and nobody can read them later, the person who runs Apiary
included.

What Apiary keeps is a log of each trip. Here's what one looks like:

| One trip | |
|---|---|
| When | Today at 2:02 pm, took 38 seconds |
| From | Claude Code |
| Asked for | claude-capable |
| Sent through | your account sam@example.com, on claude-sonnet-5-5 |
| Size | 41,200 tokens in, 1,850 out |
| Tools used | edit, shell, search |
| Ended | finished normally |
| Session tag | a7f3c9e1, the start of your agent's session id |

Not on it: what you asked, what came back, your code, your file and folder
names, or your session's name. It notes the kinds of tools your agent used,
never what they did. If your provider says no, Apiary notes what kind of no
("rate limited"), not its wording.

## Who sees the trip log

- **You.** All of yours, on your dashboard. Never anyone else's.
- **The operator.** That's the one person who runs this Apiary and invited
  you. They see the same trip logs for everyone, and nothing more. Apiary
  won't let them type into your sessions or start keeping your words.
- **Nobody else.** No ads, no analytics, no trackers, no outside scripts.
  Signing in talks to Google or GitHub. Your requests go to your own AI
  providers, under their rules.

Don't just take this page's word for it: the rules that keep your words out
are in [Apiary's public code](https://github.com/antialias/apiary).

## Everything Apiary keeps, and why

| What | Why | For how long |
|---|---|---|
| Your name, and the Google email or GitHub login you sign in with | So you can sign in | While you're a member |
| Your passkeys: the public half, and the name your browser gave each | So you can sign in | While you're a member |
| Your signed-in sessions, scrambled | So you stay signed in | 30 days |
| Each machine's name (usually its hostname) and when it last checked in | So you can see your machines and cut one off | While you're a member |
| Your AI accounts' keys and sign-ins, locked | To send your requests | Until you disconnect that account |
| What each provider says about your account: its email, plan, limits and usage | To pick an account with room to spare | Until you disconnect that account |
| Who changed your settings, what changed, and when | So nothing changes behind your back | While you're a member |
| The trip log | Your usage page, and fixing problems | 30 days |
| Your AI accounts' emails, in the server's logs and usage charts | Fixing problems | Logs roll over as they fill; charts last 90 days |
| Your IP address and browser or app, each time you visit apiary.wtf | The web server's visitor log | 90 days |
| The invite you joined with, under the name the operator gave you | To remember who was invited | For good |
| Backups of the above | So a broken disk doesn't lose your setup | Until they expire; database backups last 14 days |

## Good to know

- **Your keys are locked, and the server holds the key.** Apiary has to use
  your accounts to send your requests, so the server can unlock them. One key
  locks every member's. No page or log ever shows them, but whoever runs the
  server could unlock them. Connect accounts you're happy to trust the
  operator with.
- **Your AI accounts' emails show up for the operator.** They're in the
  server's logs and usage charts. If one of your accounts stops working, the
  operator gets an alert naming it.
- **Your agent's session id passes through.** Apiary reads it to keep a
  session on one account. The trip log keeps its first 8 characters, and the
  server's logs a few more. To tell your conversations apart, it also keeps a
  short scrambled tag made from the session and how each conversation began.
  The tag can't be turned back into words.
- **The live view shows a bit more, and saves none of it.** While a request
  runs, the live view also shows which of your machines sent it, the platform,
  the effort asked for and the request's size. That lives in memory only.
- **Your IP address stays out of the trip log.** Your agent's requests don't
  record it. Only visits to apiary.wtf do, in the web server's visitor log.

## Leaving

There's no button yet. Ask the operator, and right away:

- your AI accounts' keys, and everything about those accounts, are deleted;
- your sign-ins and passkeys are deleted;
- your name, your email and your machines' names are wiped.

What stays is a note that you were a member, under the name the operator gave
your invite. The rest fades on its own: trip logs within 30 days, logs and
charts within 90, database backups within 14.

## On the waitlist?

If you signed up on apiary.wtf before you were invited, Apiary kept what you
typed into the form, and a scrambled bit of your IP address to spot repeat
sign-ups. Ask and it's gone.

## If this page is ever wrong

This page says what the code does. If the two ever disagree, that's a bug:
tell the operator. Every change to this page is in Apiary's public code
history.
