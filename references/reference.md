Absolutely. I’ll add a dedicated **“Sources & Citations”** section to the docs and wire citations throughout the spec so every data assumption is traceable.

# What I’ll reference (and where)

**Primary data (OpticOdds API v3.0)**

* **SSE odds stream** — endpoint, parameters (`sportsbook`, `market`, `league`, `is_main`, `last_entry_id`), event types, and best-practice note to group \~10 leagues per connection. ([OpticOdds][1])
* **Active sports** — canonical sport IDs/names for the stream fan-out. ([OpticOdds][2])
* **Active leagues** — canonical league IDs/names/regions for request scoping. ([OpticOdds][3])
* **Active markets** — canonical market IDs/names for filtering. ([OpticOdds][4])
* **Sportsbooks catalog** — canonical sportsbook IDs, display names, and regional variants (used to implement your exact exclusion list while keeping base **Unibet**/**Caesars** included). ([OpticOdds][5])


# How I’ll use them inside the docs

* Each section that asserts an API behavior or field will cite the **exact OpticOdds page** it came from (e.g., the SSE “Best Practices: up to 10 leagues per connection” line cites the SSE page). ([OpticOdds][1])
* The **Sportsbook Inclusion/Exclusion** section will include a short table mapping your phrases to canonical sportsbook IDs from `/sportsbooks`, with that page cited at the top of the table. ([OpticOdds][5])
* The **Glossary & Math Primer** and **Opportunity Detection** sections will carry inline citations for: devig procedure, EV formula, arb test, and stake-split formula (the four bullets above). ([Sports Betting Dime][6], [OddsShopper][7], [Wikipedia][8], [help.smarkets.com][9])

# Citation style in the docs

* **Inline footnotes** at the end of the relevant sentence/paragraph (e.g., “group \~10 leagues per connection.”¹).
* **Reference list** at the end with the source title and URL (primary: OpticOdds API pages; secondary: method references for EV/devig/arb).
* When a statement could drift over time (API behavior, parameter names), the doc will note **“Source of truth: OpticOdds API v3.0”** and link to the exact page. ([OpticOdds][1])

If you’d like, I can also include a tiny **“Verification checklist”** in the runbook (e.g., “Open `/sportsbooks`, confirm exclusions present; sample a couple IDs like `betrivers_new_york_`, `betfair_exchange_lay_`”). ([OpticOdds][5])

[1]: https://developer.opticodds.com/reference/get_stream-odds-sport "/stream/odds/{sport}"
[2]: https://developer.opticodds.com/reference/get_sports-active "/sports/active"
[3]: https://developer.opticodds.com/reference/get_leagues-active "/leagues/active"
[4]: https://developer.opticodds.com/reference/get_markets-active "/markets/active"
[5]: https://developer.opticodds.com/reference/get_sportsbooks "/sportsbooks"
