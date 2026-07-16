# Example project facts: PB Web Tools

A worked example of the **Phase 1: Learn the project** facts, filled in for the
PB Web Tools repo. Use it as a template for what to discover about any project.
Do not assume these facts apply anywhere else; rediscover them per project.

## Reach (how a tester gets to the product)
The builder pages live under `public/`. The live site is team-only at
`https://pb-webtools.web.app`. For auth-guarded pages served over http, follow
the recipe in the repo's `CLAUDE.md` for standing up an unguarded local copy so a
tester can reach the builder.

## Test tooling
Chromium and Playwright are preconfigured. The browsers live at
`PLAYWRIGHT_BROWSERS_PATH=/opt/pw-browsers`, so do not run `playwright install`.
Testers should script real clicks and typing and capture screenshots.

## What the product is (this shapes what a good fix looks like)
A set of embeddable, dependency-free HTML widgets that people paste into their
own sites. So fixes must stay surgical: no new framework, no phone-home, nothing
that bloats the embed. Security matters doubly because the output is HTML people
paste into their own pages: no injected script or XSS in generated embeds.

## Ship path (fixes mode)
Commit to `main`; GitHub Actions auto-deploys. Verify before pushing. Never run a
bare `firebase deploy`. If you changed Firestore rules, deploy them manually.
Respect every rule in `CLAUDE.md`. Because this is direct-to-production, the
Phase 7 confirmation gate applies before the push to `main`.
