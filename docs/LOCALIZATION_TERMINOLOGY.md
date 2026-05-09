# PowerSim Localization Terminology

This note records the Georgian/English terminology choices used by the HTML interface localization.

| English term | Georgian UI term | Notes |
| --- | --- | --- |
| Power system planning | ენერგოსისტემის დაგეგმვა | Professional planning context. |
| Scenario simulation | სცენარის სიმულაცია | Kept close to common technical usage. |
| Rolling horizon | Rolling horizon | Kept in English in the UI because the method name is often used unchanged; explanatory copy uses `მოძრავი ჰორიზონტი` if Giorgi prefers full Georgian. |
| Gas cap | გაზის ლიმიტი | Short UI form; budget panels use annual/monthly budget wording. |
| Annual gas budget | წლიური გაზის ბიუჯეტი | Used for annual cap labels. |
| Monthly gas budget | თვიური გაზის ბიუჯეტი | Used for monthly cap labels. |
| Unit commitment | აგრეგატების ჩართვა/გამორთვის ოპტიმიზაცია | Abbreviation UC remains in technical solver note. |
| Economic dispatch | ეკონომიკური დისპეტჩირება | Abbreviation ED remains in technical solver note. |
| BESS | BESS | Industry acronym retained. |
| Reservoir | წყალსაცავი | Hydro asset labels use `წყალსაცავიანი ჰესი`. |
| Hydro generation | ჰიდროგენერაცია | Asset labels distinguish reservoir and run-of-river hydro. |
| Thermal generation | თბოგენერაცია | Used for thermal asset type. |
| Imports | იმპორტი | Used for import assets and result categories. |
| Wind | ქარი | Used for wind assets. |
| Solar | მზე | Used for solar assets. |
| Curtailment | შეზღუდვა / შემცირება | `Curtailed` remains close to source KPI where space is limited. |
| Unserved energy | მიუწოდებელი ენერგია | Used for results KPI. |
| Capacity factor | სიმძლავრის გამოყენების კოეფიციენტი | Table keeps compact `CF%`. |
| Scenario comparison | სცენარების შედარება | Used in Compare tab. |
| Dispatch results | დისპეტჩირების შედეგები | Used for dispatch result context. |
| Validation | ვალიდაცია | Common technical UI term. |
| Export report | ანგარიშის ექსპორტი | Used as report/export wording. |

## Terms for Giorgi to approve

- **Rolling horizon**: currently retained as `Rolling horizon`; approve whether to switch all visible copy to `მოძრავი ჰორიზონტი`.
- **Unit commitment**: suggested Georgian expansion is `აგრეგატების ჩართვა/გამორთვის ოპტიმიზაცია`; approve whether `Unit Commitment`/`UC` should remain in headings.
- **Economic dispatch**: `ეკონომიკური დისპეტჩირება` is used as the Georgian equivalent; approve if `ეკონომიკური განაწილება` is preferred.
- **Curtailment**: Georgian energy-sector usage varies between `შეზღუდვა` and `შემცირება`; approve preferred term for final reports.
