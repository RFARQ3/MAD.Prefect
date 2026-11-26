# EnvBlock downstream regression coverage

## Running the regression contract

```powershell
poetry run pytest tests/test_env_block.py tests/test_env_block_downstream.py tests/test_envblock_downstream_usage.py tests/test_envblock_coverage_inventory.py
```

The regression contract is the EnvBlock boundary represented by every definition and usage in `observed-envblock-downstream-usage.md`. It protects loaded values and types, field defaults and constraints, prefix selection, missing and invalid inputs, cached identity, private state, saved-block selection, and the consumer operations that depend on them.

The source inventory contains **48 definition variants and 177 usages**. Definitions 32–35 and usages 016–020 belong to an independent EnvBlock implementation. They are explicitly accounted for but cannot regress through this shared-library change. The other **44 definitions and 172 usages** are mapped below.

The full source audit is a local review artifact and is not required to run tests. Only the synthetic field declarations and consumer methods used by focused behavioural tests are included in `tests/fixtures/envblock_consumer_examples.json`. The inventory below preserves the review mapping.

## Test families

| Code | Executable coverage |
| --- | --- |
| D | `test_env_block_downstream.py` and `test_env_block.py`: representative models exercise each distinct field type, default, constraint, prefix, saved-load path, cache and private-state behaviour once. The rows below map downstream implementations to these shared behaviours; they do not generate per-instance tests. |
| N | D's integer/float/default/constraint cases protect numeric arithmetic, date-window sizes, pagination limits and concurrency inputs. Numeric consumers receive actual numbers, not environment strings. |
| L | `test_usage114_override_preserves_remapping_and_explicit_precedence`, `test_definition09_override_loads_real_model_and_calendar_values`, `test_definition09_override_loads_and_caches_saved_block`, and `test_definition09_credentials_forward_file_and_optional_subject`. |
| P | `test_definition28_delimited_report_properties_keep_string_semantics`, `test_definition42_json_secret_drives_project_keys_token_and_headers`, `test_definition42_invalid_json_is_rejected_by_consumer`, and `test_definitions24_25_company_url_templates_receive_unchanged_strings`; L covers calendar parsing. |
| R | `test_usage155_registration_and_cached_repeat`: actual registration method, all three remote-protocol branches plus local path, secret forwarding, mutation and cached repeat. |
| S | `test_definitions40_41_save_env_preserves_typed_updates`: both variants, saved-name present/absent, typed mutation retained by cache, mocked save and auth secret forwarding. |
| T | `test_usage158_cache_expiration_branches_use_loaded_types`: both variants, absent prior run, normal/delta mode and elapsed time below/above the full-cache duration; D covers environment dates/durations and None defaults. |
| B | `test_usage129_backfill_controls_real_consumer_branch`: actual consumer branch for false/true/0/1, numeric partition arguments and expected backfill invocation counts. |
| W | `test_client_wrapper_retains_cached_settings_and_mutations`: extracted client wrappers pass the loaded model into a client and cached settings retain deliberate scope mutation. D covers all corresponding model fields. |
| C | `test_usages138_150_scopes_gate_work_and_integer_cache_minutes`: extracted scope and cache-selection operations use preserved strings and typed durations for both constrained definition variants; D covers older scope-only declarations. |
| X | Independent local implementation, explicitly outside the changed shared-library inheritance tree. |

Each row marked D or N represents the relevant EnvBlock input/output contract, not execution of every unrelated database, HTTP, retry, orchestration or storage algorithm appearing around that load call. The focused method tests execute the actual captured method bodies with offline collaborators where EnvBlock-specific branching or mutation makes a field-only assertion insufficient.

## Definition inventory

| Definition | Service | Regression families |
| --- | --- | --- |
| 01 | Service01 | D |
| 02 | Service01 | D |
| 03 | Service02 | D, W |
| 04 | Service03 | D |
| 05 | Service04 | D |
| 06 | Service05 | D, R |
| 07 | Service06 | D |
| 08 | Service07 | D |
| 09 | Service08 | D, L, P |
| 10 | Service09 | D, W |
| 11 | Service10 | D |
| 12 | Service10 | D |
| 13 | Service10 | D |
| 14 | Service10 | D |
| 15 | Service10 | D |
| 16 | Service10 | D |
| 17 | Service10 | D |
| 18 | Service11 | D |
| 19 | Service12 | D |
| 20 | Service12 | D |
| 21 | Service13 | D |
| 22 | Service14 | D |
| 23 | Service15 | D |
| 24 | Service16 | D, P |
| 25 | Service17 | D, P |
| 26 | Service18 | D |
| 27 | Service19 | D |
| 28 | Service20 | D, P |
| 29 | Service21 | D |
| 30 | Service22 | D, B |
| 31 | Service22 | D |
| 32 | Service23 | X |
| 33 | Service23 | X |
| 34 | Service23 | X |
| 35 | Service23 | X |
| 36 | Service24 | D |
| 37 | Service25 | D |
| 38 | Service25 | D |
| 39 | Service26 | D |
| 40 | Service27 | D, S, T |
| 41 | Service27 | D, S, T |
| 42 | Service28 | D, P |
| 43 | Service29 | D |
| 44 | Service29 | D |
| 45 | Service31 | D, C |
| 46 | Service31 | D, C |
| 47 | Service31 | D, C |
| 48 | Service30 | D |

## Usage inventory

| Usage | Service | Owning definitions | Regression families |
| --- | --- | --- | --- |
| 001 | Service14 | 22 | D (loaded values, secrets, identity and mutable state) |
| 002 | Service14 | 22 | D (loaded values, secrets, identity and mutable state) |
| 003 | Service14 | 22 | N |
| 004 | Service14 | 22 | D (loaded values, secrets, identity and mutable state) |
| 005 | Service14 | 22 | D (loaded values, secrets, identity and mutable state) |
| 006 | Service14 | 22 | D (loaded values, secrets, identity and mutable state) |
| 007 | Service14 | 22 | N |
| 008 | Service14 | 22 | D (loaded values, secrets, identity and mutable state) |
| 009 | Service14 | 22 | D (loaded values, secrets, identity and mutable state) |
| 010 | Service14 | 22 | D (loaded values, secrets, identity and mutable state) |
| 011 | Service14 | 22 | D (loaded values, secrets, identity and mutable state) |
| 012 | Service14 | 22 | D (loaded values, secrets, identity and mutable state) |
| 013 | Service11 | 18 | D (loaded values, secrets, identity and mutable state) |
| 014 | Service11 | 18 | D (loaded values, secrets, identity and mutable state) |
| 015 | Service11 | 18 | N |
| 016 | Service23 | 32, 33, 34, 35 | X |
| 017 | Service23 | 32, 33, 34, 35 | X |
| 018 | Service23 | 32, 33, 34, 35 | X |
| 019 | Service23 | 32, 33, 34, 35 | X |
| 020 | Service23 | 32, 33, 34, 35 | X |
| 021 | Service26 | 39 | D (loaded values, secrets, identity and mutable state) |
| 022 | Service26 | 39 | D (loaded values, secrets, identity and mutable state) |
| 023 | Service26 | 39 | D (loaded values, secrets, identity and mutable state) |
| 024 | Service26 | 39 | D (private state and cached identity) |
| 025 | Service01 | 01, 02 | D (loaded values, secrets, identity and mutable state) |
| 026 | Service01 | 01, 02 | D (loaded values, secrets, identity and mutable state) |
| 027 | Service01 | 01, 02 | D (loaded values, secrets, identity and mutable state) |
| 028 | Service01 | 01, 02 | D (loaded values, secrets, identity and mutable state) |
| 029 | Service01 | 01, 02 | D (loaded values, secrets, identity and mutable state) |
| 030 | Service01 | 01, 02 | D (loaded values, secrets, identity and mutable state) |
| 031 | Service01 | 01, 02 | D (loaded values, secrets, identity and mutable state) |
| 032 | Service01 | 01, 02 | D (loaded values, secrets, identity and mutable state) |
| 033 | Service01 | 01, 02 | D (loaded values, secrets, identity and mutable state) |
| 034 | Service01 | 01, 02 | D (loaded values, secrets, identity and mutable state) |
| 035 | Service01 | 01, 02 | D (loaded values, secrets, identity and mutable state) |
| 036 | Service01 | 01, 02 | D (loaded values, secrets, identity and mutable state) |
| 037 | Service01 | 01, 02 | D (loaded values, secrets, identity and mutable state) |
| 038 | Service01 | 01, 02 | D (loaded values, secrets, identity and mutable state) |
| 039 | Service01 | 01, 02 | D (loaded values, secrets, identity and mutable state) |
| 040 | Service01 | 01, 02 | D (saved block) |
| 041 | Service15 | 23 | D (loaded values, secrets, identity and mutable state) |
| 042 | Service15 | 23 | D (loaded values, secrets, identity and mutable state) |
| 043 | Service15 | 23 | D (private state and cached identity) |
| 044 | Service15 | 23 | D (loaded values, secrets, identity and mutable state) |
| 045 | Service09 | 10 | W |
| 046 | Service09 | 10 | W |
| 047 | Service09 | 10 | W |
| 048 | Service09 | 10 | D (explicit mapping and secrets) |
| 049 | Service25 | 37, 38 | D (loaded values, secrets, identity and mutable state) |
| 050 | Service25 | 37, 38 | D (loaded values, secrets, identity and mutable state) |
| 051 | Service25 | 37, 38 | D (loaded values, secrets, identity and mutable state) |
| 052 | Service24 | 36 | D (loaded values, secrets, identity and mutable state) |
| 053 | Service24 | 36 | N |
| 054 | Service18 | 26 | D (loaded values, secrets, identity and mutable state) |
| 055 | Service18 | 26 | D (loaded values, secrets, identity and mutable state) |
| 056 | Service18 | 26 | D (loaded values, secrets, identity and mutable state) |
| 057 | Service03 | 04 | D (loaded values, secrets, identity and mutable state) |
| 058 | Service13 | 21 | D (loaded values, secrets, identity and mutable state) |
| 059 | Service13 | 21 | D (loaded values, secrets, identity and mutable state) |
| 060 | Service13 | 21 | D (loaded values, secrets, identity and mutable state) |
| 061 | Service20 | 28 | D (loaded values, secrets, identity and mutable state) |
| 062 | Service20 | 28 | P |
| 063 | Service20 | 28 | D (loaded values, secrets, identity and mutable state) |
| 064 | Service20 | 28 | P |
| 065 | Service20 | 28 | P |
| 066 | Service20 | 28 | P |
| 067 | Service02 | 03 | W |
| 068 | Service02 | 03 | W |
| 069 | Service02 | 03 | W |
| 070 | Service02 | 03 | W |
| 071 | Service02 | 03 | W |
| 072 | Service02 | 03 | W |
| 073 | Service02 | 03 | W |
| 074 | Service02 | 03 | W |
| 075 | Service02 | 03 | W |
| 076 | Service02 | 03 | W |
| 077 | Service02 | 03 | W |
| 078 | Service02 | 03 | W |
| 079 | Service02 | 03 | W |
| 080 | Service02 | 03 | W |
| 081 | Service02 | 03 | W |
| 082 | Service02 | 03 | D (explicit mapping and secrets) |
| 083 | Service02 | 03 | W |
| 084 | Service02 | 03 | W |
| 085 | Service02 | 03 | W |
| 086 | Service02 | 03 | W |
| 087 | Service02 | 03 | W |
| 088 | Service02 | 03 | W |
| 089 | Service02 | 03 | W |
| 090 | Service02 | 03 | W |
| 091 | Service02 | 03 | W |
| 092 | Service02 | 03 | W |
| 093 | Service16 | 24 | P |
| 094 | Service21 | 29 | D (loaded values, secrets, identity and mutable state) |
| 095 | Service30 | 48 | D (loaded values, secrets, identity and mutable state) |
| 096 | Service30 | 48 | D (loaded values, secrets, identity and mutable state) |
| 097 | Service30 | 48 | N |
| 098 | Service30 | 48 | D (loaded values, secrets, identity and mutable state) |
| 099 | Service30 | 48 | D (loaded values, secrets, identity and mutable state) |
| 100 | Service30 | 48 | N |
| 101 | Service30 | 48 | N |
| 102 | Service06 | 07 | D (loaded values, secrets, identity and mutable state) |
| 103 | Service06 | 07 | D (loaded values, secrets, identity and mutable state) |
| 104 | Service06 | 07 | D (loaded values, secrets, identity and mutable state) |
| 105 | Service12 | 19, 20 | D (loaded values, secrets, identity and mutable state) |
| 106 | Service12 | 19, 20 | D (loaded values, secrets, identity and mutable state) |
| 107 | Service12 | 19, 20 | D (loaded values, secrets, identity and mutable state) |
| 108 | Service12 | 19, 20 | D (loaded values, secrets, identity and mutable state) |
| 109 | Service12 | 19, 20 | D (loaded values, secrets, identity and mutable state) |
| 110 | Service12 | 19, 20 | D (loaded values, secrets, identity and mutable state) |
| 111 | Service12 | 19, 20 | D (loaded values, secrets, identity and mutable state) |
| 112 | Service12 | 19, 20 | D (loaded values, secrets, identity and mutable state) |
| 113 | Service08 | 09 | L, P |
| 114 | Service08 | 09 | L, P |
| 115 | Service08 | 09 | L, P |
| 116 | Service08 | 09 | L, P |
| 117 | Service08 | 09 | L, P |
| 118 | Service08 | 09 | L, P |
| 119 | Service29 | 43, 44 | D (loaded values, secrets, identity and mutable state) |
| 120 | Service29 | 43, 44 | D (loaded values, secrets, identity and mutable state) |
| 121 | Service29 | 43, 44 | N |
| 122 | Service28 | 42 | P |
| 123 | Service28 | 42 | P |
| 124 | Service28 | 42 | P |
| 125 | Service28 | 42 | P |
| 126 | Service28 | 42 | P |
| 127 | Service28 | 42 | P |
| 128 | Service28 | 42 | P |
| 129 | Service22 | 30, 31 | B |
| 130 | Service22 | 30, 31 | N |
| 131 | Service22 | 30, 31 | N |
| 132 | Service22 | 30, 31 | N |
| 133 | Service19 | 27 | D (loaded values, secrets, identity and mutable state) |
| 134 | Service19 | 27 | D (loaded values, secrets, identity and mutable state) |
| 135 | Service17 | 25 | P |
| 136 | Service17 | 25 | P |
| 137 | Service31 | 45, 46, 47 | D (loaded values, secrets, identity and mutable state) |
| 138 | Service31 | 45, 46, 47 | C |
| 139 | Service31 | 45, 46, 47 | N |
| 140 | Service31 | 45, 46, 47 | N |
| 141 | Service31 | 45, 46, 47 | N |
| 142 | Service31 | 45, 46, 47 | N |
| 143 | Service31 | 45, 46, 47 | N |
| 144 | Service31 | 45, 46, 47 | D (explicit mapping and secrets) |
| 145 | Service31 | 45, 46, 47 | D (saved block) |
| 146 | Service31 | 45, 46, 47 | D (required field failure) |
| 147 | Service31 | 45, 46, 47 | D (loaded values, secrets, identity and mutable state) |
| 148 | Service31 | 45, 46, 47 | D (loaded values, secrets, identity and mutable state) |
| 149 | Service31 | 45, 46, 47 | D (loaded values, secrets, identity and mutable state) |
| 150 | Service31 | 45, 46, 47 | C |
| 151 | Service31 | 45, 46, 47 | D (loaded values, secrets, identity and mutable state) |
| 152 | Service07 | 08 | D (loaded values, secrets, identity and mutable state) |
| 153 | Service07 | 08 | D (loaded values, secrets, identity and mutable state) |
| 154 | Service07 | 08 | N |
| 155 | Service05 | 06 | R |
| 156 | Service27 | 40, 41 | D (loaded values, secrets, identity and mutable state) |
| 157 | Service27 | 40, 41 | S, T |
| 158 | Service27 | 40, 41 | T |
| 159 | Service27 | 40, 41 | T |
| 160 | Service27 | 40, 41 | T |
| 161 | Service27 | 40, 41 | D (loaded values, secrets, identity and mutable state) |
| 162 | Service27 | 40, 41 | S, T |
| 163 | Service27 | 40, 41 | T |
| 164 | Service27 | 40, 41 | T |
| 165 | Service04 | 05 | D (loaded values, secrets, identity and mutable state) |
| 166 | Service04 | 05 | D (loaded values, secrets, identity and mutable state) |
| 167 | Service10 | 11, 12, 13, 14, 15, 16, 17 | D (loaded values, secrets, identity and mutable state) |
| 168 | Service10 | 11, 12, 13, 14, 15, 16, 17 | N |
| 169 | Service10 | 11, 12, 13, 14, 15, 16, 17 | D (loaded values, secrets, identity and mutable state) |
| 170 | Service10 | 11, 12, 13, 14, 15, 16, 17 | D (loaded values, secrets, identity and mutable state) |
| 171 | Service10 | 11, 12, 13, 14, 15, 16, 17 | D (loaded values, secrets, identity and mutable state) |
| 172 | Service10 | 11, 12, 13, 14, 15, 16, 17 | D (loaded values, secrets, identity and mutable state) |
| 173 | Service10 | 11, 12, 13, 14, 15, 16, 17 | D (loaded values, secrets, identity and mutable state) |
| 174 | Service10 | 11, 12, 13, 14, 15, 16, 17 | D (loaded values, secrets, identity and mutable state) |
| 175 | Service10 | 11, 12, 13, 14, 15, 16, 17 | N |
| 176 | Service10 | 11, 12, 13, 14, 15, 16, 17 | N |
| 177 | Service10 | 11, 12, 13, 14, 15, 16, 17 | D (loaded values, secrets, identity and mutable state) |

## Intentional conversion changes

- Boolean strings now follow Pydantic validation: false/0 disable the setting and true/1 enable it. Invalid boolean strings fail validation instead of becoming truthy.
- A supplied string for `int | str` follows native union selection and remains a string. No captured shared-library public field uses this union; the base regression explicitly locks the intended new rule.
- Invalid supplied values raise Pydantic `ValidationError`, a `ValueError` subclass. Missing required values retain the existing environment-variable `ValueError` contract.
- Optional dates and duration strings use Pydantic's supported conversion. Their defaults and saved-block typed values remain protected.

## Evidence boundaries

The audit source is anonymised: literal environment names, delimiters, encodings, URLs, format strings and SDK names were replaced, and imports/supporting implementations were omitted. Its own header warns that exact original literal behavior cannot be inferred from these excerpts. Tests exercise representative declarations and selected captured consumer methods with synthetic values; they do not reconstruct unknown original spellings or claim real SDK integration verification.

The legacy-alias test checks the captured override's forwarding and precedence separately from the real inherited loader because anonymisation disconnects alias literals from field names. Saved-block network access is replaced by a controlled loader; returned object identity, caching and field preservation are tested.

This is a locked regression contract for the observed downstream EnvBlock use, not mathematical proof for every possible environment value, dependency version, unobserved future implementation or external service behavior. No customer system or credential file is needed to run it.

## Progress

- Consolidated verification: **105 EnvBlock and coverage cases passed; 188 tests passed across the repository**. Type checking reported zero errors and whitespace checks passed. The four existing Prefect test-class registration warnings remain.

- Completed independent reading of all 4,706 source lines and classified all definition and usage IDs.
- Independently inspected the complete declaration suite and focused consumer method tests against that inventory; all observed EnvBlock contracts are represented. The mapped unrelated application algorithms remain outside the test boundary described above.
- Added the definition and usage coverage inventory, including explicit independent-implementation exclusions.
- Added executable guards requiring every definition and usage to appear exactly once, match its service and owning definitions, retain the explicit exclusions, and reference existing test names.
- Negative control: temporarily restoring `bool(raw)` for BACKFILL makes the actual Usage129 false and 0 cases fail (four materializations instead of one). True and 1 still pass. Removing the mutation restores the passing suite.
- Prior verification: the exhaustive per-definition/per-field matrix passed **1,878 tests** before consolidation. That one-time compatibility exercise is evidence for the PR, not the size of the maintained suite. The maintained suite now tests distinct behaviours and retains the coverage map without the generated definition fixture.
- Additional negative controls: forcing class-name-only prefix resolution and disabling instance caching each failed the corresponding regression. These checks used temporary in-process mutations and left production source unchanged.

## Next Steps

- Release the reviewed shared-library fix and update the consuming application dependency.
- When the source inventory changes, identify any new behaviour and extend its regression family; do not add repeated tests for another instance of an existing behaviour.

## Blockers & Risks

- The anonymised source cannot establish exact original literal spellings or external SDK/service behavior; these are outside the compatibility evidence above.
- The intentional conversion changes above must remain explicit in release notes.
