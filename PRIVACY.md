# Privacy

*For members of the Apiary hive at apiary.wtf. Last changed 2026-11-19.*

Apiary sends your coding agent's requests to the AI accounts you connect. One
person, the operator, runs the hive and invites its members. This page says
what Apiary keeps about you, for how long, and who can see it. It describes
what the code does: where the two disagree, the code has a bug.

## In short

- Your prompts and the replies to them pass through Apiary on their way to and
  from your provider. Apiary keeps neither. They are not stored, not logged and
  not shown to anyone, the operator included.
- Apiary keeps metadata about each request: which of your accounts served it,
  which model, how many tokens, how long it took and how it ended.
- You see your own metadata on your dashboard. The operator sees the same
  metadata for every member, and nothing more.

## What passes through and is not kept

Prompts, replies, system prompts, tool calls and their results, file names and
folders, and session names. They are held in memory only while a request is in
flight. They are never written to disk, to a log, to the live feed or to a
dashboard, yours or the operator's. A provider's error message is not kept
either: Apiary keeps only its type, such as "rate limited".

## What Apiary keeps

**Your account**, kept while you are a member:

- your display name;
- for each way you sign in, Google's or GitHub's id for you, and your Google
  address or GitHub login;
- your passkeys: the public key, and a name taken from your browser;
- the invite you joined with: the name the operator gave it and when you used it.

Signed-in sessions are stored as a hash of their token, with when they were
last used, and are deleted after 30 days.

**Your machines**: for each machine you enrol, the name it reports (usually its
hostname) and when it last renewed its credential. The credential is stored
only as a hash.

**Your connected accounts**, kept while each is connected:

- its sign-in tokens or API key, encrypted at rest (AES-256-GCM). Apiary must
  use them to send your requests, so its server holds the key, one key for
  every member's credentials. No page or log shows them;
- what the provider reports about the account: its email address, plan, rate
  tier, and usage windows with their resets;
- a record of changes to your settings: what changed, who changed it, and when.

Removing an account deletes its credentials.

**Each request**, kept 30 days:

- when it started and how long it took;
- which of your machines sent it, the client and the platform;
- the model and effort asked for, and the class, lane and model that served it;
- which of your accounts served it, the token counts and the request's size;
- how it ended, with the error type;
- a short session id, made by hashing your agent's own session id.

Apiary's router also receives your agent's session id itself, to keep a
session on one account, and its logs keep the first characters of it. The live
feed holds the latest few thousand events in memory only.

**Logs and metrics.** Apiary's server logs name your connected accounts by
their email address, and the operator's usage metrics are labelled with them.
Logs rotate by size, and metrics are kept 90 days. Alerts about your accounts,
such as an account that stopped working, go to the operator and name the
account.

Apiary's web server logs each visit to apiary.wtf: the time, the IP address it
came from, the address asked for, the result and the browser's name. That log
is kept 90 days. Your coding agent's requests are not in it, and Apiary's
router does not see the IP address they come from.

**The waitlist.** If you signed up on apiary.wtf before you were invited,
Apiary keeps everything you entered in the form, and a short hash of your IP
address used to spot repeat sign-ups, until you ask for it to be removed.

## Who can see what

- **You** see everything above about you on your dashboard, and never another
  member's.
- **The operator** sees the same metadata for every member. The operator
  cannot read your prompts or replies, since they are not kept. Apiary also
  refuses to let the operator type into your sessions or switch your account
  to a mode that keeps content.
- **No one else.** Apiary sends nothing to analytics, error-reporting or
  tracking services, and its pages load no third-party scripts or fonts.
  Signing in talks to Google or GitHub. Your requests go to the providers
  whose accounts you connected, under those providers' own terms.

## Leaving

There is no self-serve delete yet: ask the operator. Removing a member:

- deletes, at once, your connected accounts' credentials and everything Apiary
  keeps about those accounts;
- deletes your sign-in methods and passkeys, clears your machines' names and
  your name and email from your account, and keeps only a record that the
  account was removed. The invite you used, and the record that you were a
  member, keep the name the operator gave you;
- leaves the request records to expire within 30 days, the server logs to
  rotate, and the web server's log and the metrics to expire within 90 days;
- leaves copies in the server's backups until those expire. Apiary's daily
  database backups are deleted after 14 days.

## Changes

This page lives in Apiary's public repository, and every change to it is in
that repository's history.
