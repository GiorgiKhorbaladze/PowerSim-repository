// UI language support. English is the source language; Georgian (ka) is a
// translation table of exact UI phrases. Data values (ids, scenario names,
// codes, numbers) are never translated because they are not in the table.
// Terminology follows docs/LOCALIZATION_TERMINOLOGY.md.

const KA = {
  // navigation and header
  "Overview": "მიმოხილვა", "Project": "პროექტი", "Assets": "აქტივები", "Scenarios": "სცენარები",
  "Validation": "ვალიდაცია", "Runs": "გაშვებები", "Results": "შედეგები", "Compare": "შედარება",
  "Reference data": "საცნობარო მონაცემები", "Capabilities & limits": "შესაძლებლობები და შეზღუდვები",
  "Workspace": "სამუშაო სივრცე", "Language": "ენა", "Backend": "სერვერი", "Electricity-system planning and optimization": "ენერგოსისტემის დაგეგმვა და ოპტიმიზაცია",
  "Validate": "ვალიდაცია", "Save project": "პროექტის შენახვა", "Export project JSON": "პროექტის JSON ექსპორტი",
  "No project open": "პროექტი არ არის გახსნილი", "Unsaved changes": "შეუნახავი ცვლილებები", "All changes saved": "ყველა ცვლილება შენახულია",
  "Validation failed": "ვალიდაცია ვერ გაიარა", "Draft - unsaved changes": "მონახაზი - შეუნახავი ცვლილებები",
  "Draft - valid, unsaved": "მონახაზი - ვალიდური, შეუნახავი", "Draft - not run-ready, unsaved": "მონახაზი - გასაშვებად არ არის მზად, შეუნახავი",
  "Saved - valid, run-ready": "შენახულია - ვალიდური, მზადაა გასაშვებად", "Saved - not run-ready": "შენახულია - გასაშვებად არ არის მზად",
  "Saved - not validated": "შენახულია - ვალიდაცია არ ჩატარებულა", "Backend unreachable": "სერვერი მიუწვდომელია",
  "Connecting...": "დაკავშირება...", "Backend ready": "სერვერი მზადაა", "No executor configured": "გამშვები არ არის კონფიგურირებული",
  "Solver unavailable (HiGHS missing)": "ამომხსნელი მიუწვდომელია (HiGHS არ არის)", "Skip to content": "შიგთავსზე გადასვლა",
  // overview
  "Load the sanitized Georgia demonstration or create a project from a typed project document. Validate and save it, launch a run, then review QA-gated results.":
    "ჩატვირთეთ საქართველოს სადემონსტრაციო პროექტი ან შექმენით პროექტი ტიპიზებული დოკუმენტიდან. შეამოწმეთ და შეინახეთ, გაუშვით და ნახეთ QA-ით შემოწმებული შედეგები.",
  "Open a saved project": "შენახული პროექტის გახსნა", "This workspace has no saved projects yet.": "ამ სამუშაო სივრცეში შენახული პროექტი ჯერ არ არის.",
  "Saved projects": "შენახული პროექტები", "Open": "გახსნა", "Sample projects": "სანიმუშო პროექტები",
  "Load sample": "ნიმუშის ჩატვირთვა", "Reload sample (overwrite)": "ნიმუშის ხელახლა ჩატვირთვა (გადაწერა)", "Open saved copy": "შენახული ასლის გახსნა",
  "Create a project from a typed project document": "პროექტის შექმნა ტიპიზებული დოკუმენტიდან",
  "The document must follow the PowerSim project contract (schemas/v1). It is validated on the server before it is saved.":
    "დოკუმენტი უნდა შეესაბამებოდეს PowerSim-ის პროექტის კონტრაქტს (schemas/v1). შენახვამდე მას სერვერი ამოწმებს.",
  "Project JSON file": "პროექტის JSON ფაილი", "Project document": "პროექტის დოკუმენტი",
  "Paste a PowerSim project JSON document": "ჩასვით PowerSim-ის პროექტის JSON დოკუმენტი",
  "Insert minimal template": "მინიმალური შაბლონის ჩასმა", "Validate document": "დოკუმენტის ვალიდაცია", "Create project": "პროექტის შექმნა",
  "Supported workflows": "მხარდაჭერილი ამოცანები", "Not supported: ": "არ არის მხარდაჭერილი: ", "Not supported": "არ არის მხარდაჭერილი",
  "No project is open.": "პროექტი არ არის გახსნილი.", "Open or create a project": "პროექტის გახსნა ან შექმნა",
  // project
  "Study definition": "კვლევის განსაზღვრა", "Project id": "პროექტის id", "Name": "სახელი", "Workflow": "ამოცანა", "Currency": "ვალუტა",
  "The id is the project identity; change it in the project document.": "id პროექტის იდენტიფიკატორია; შეცვალეთ პროექტის დოკუმენტში.",
  "Time axis": "დროის ღერძი", "The time axis must stay consistent with every profile; edit it in the project document.":
    "დროის ღერძი ყველა პროფილს უნდა შეესაბამებოდეს; შეცვალეთ პროექტის დოკუმენტში.",
  "Solver settings": "ამომხსნელის პარამეტრები",
  "These typed settings are passed to the solver. Blank fields use the solver defaults. Compatibility settings inside legacy_payload are not used by the application executor.":
    "ეს პარამეტრები გადაეცემა ამომხსნელს. ცარიელი ველები ნაგულისხმევ მნიშვნელობებს იყენებს. legacy_payload-ის პარამეტრები გაშვებისას არ გამოიყენება.",
  "Metadata and provenance": "მეტამონაცემები და წარმომავლობა", "No additional metadata.": "დამატებითი მეტამონაცემები არ არის.",
  "Reference-data imports": "საცნობარო მონაცემების იმპორტები", "Project document (advanced JSON editor)": "პროექტის დოკუმენტი (JSON რედაქტორი)",
  "Edit any part of the typed project. Applying replaces the working copy; nothing is saved until you choose Save project.":
    "შეცვალეთ პროექტის ნებისმიერი ნაწილი. გამოყენება ცვლის სამუშაო ასლს; შენახვა ხდება მხოლოდ „პროექტის შენახვით“.",
  "Apply document to working copy": "დოკუმენტის გამოყენება სამუშაო ასლზე",
  // assets
  "Disabled assets stay in the project but are excluded from runs. Reference-catalogue assets are imported disabled and must be reviewed before they can be enabled in a run.":
    "გამორთული აქტივები პროექტში რჩება, მაგრამ გაშვებაში არ მონაწილეობს. საცნობარო კატალოგიდან შემოტანილი აქტივები გამორთულია და ჩართვამდე შემოწმებას საჭიროებს.",
  "Add asset": "აქტივის დამატება", "This project has no assets.": "პროექტს აქტივები არ აქვს.", "Enabled": "ჩართული", "Id": "Id",
  "Kind": "ტიპი", "Min MW": "მინ. მგვტ", "Max MW": "მაქს. მგვტ", "Bus": "კვანძი", "Profiles": "პროფილები", "Source": "წყარო", "Edit": "რედაქტირება",
  "Project": "პროექტი", "enabled": "ჩართული", "disabled": "გამორთული", "reviewed": "შემოწმებული", "requires review": "საჭიროებს შემოწმებას",
  "Reference catalogue ": "საცნობარო კატალოგი ", "New asset": "ახალი აქტივი", "Asset id": "აქტივის id",
  "Min capacity (MW)": "მინიმალური სიმძლავრე (მგვტ)", "Max capacity (MW)": "მაქსიმალური სიმძლავრე (მგვტ)",
  "Profile references": "პროფილის მითითებები", "The first profile is used as availability profile.": "პირველი პროფილი ხელმისაწვდომობის პროფილად გამოიყენება.",
  "Variable O&M (currency/MWh)": "ცვლადი ხარჯი VOM (ვალუტა/მგვტ.სთ)", "Heat rate (GJ/MWh)": "სპეციფიკური ხარჯი (გჯ/მგვტ.სთ)",
  "Fuel price (USD/MMBtu)": "საწვავის ფასი (USD/MMBtu)", "Fuel type": "საწვავის ტიპი", "Enabled in runs": "ჩართულია გაშვებებში",
  "Add to project": "პროექტში დამატება", "Apply changes": "ცვლილებების გამოყენება", "Cancel": "გაუქმება", "Remove asset": "აქტივის წაშლა",
  "The asset was not applied": "აქტივი არ შეიცვალა", "Reference values require user review": "საცნობარო მნიშვნელობები შემოწმებას საჭიროებს",
  "Reviewed reference values": "შემოწმებული საცნობარო მნიშვნელობები", "Field origins": "ველების წარმომავლობა",
  "Field": "ველი", "Status": "სტატუსი", "Origin": "წარმომავლობა", "Reason / note": "მიზეზი / შენიშვნა",
  "Attached reference metadata (not used by the solver)": "თანდართული საცნობარო მეტამონაცემები (ამომხსნელი არ იყენებს)",
  "Review note (required)": "შემოწმების შენიშვნა (სავალდებულო)", "What did you check, against which approved source?": "რა შეამოწმეთ და რომელ დამტკიცებულ წყაროსთან?",
  "Mark parameters reviewed": "პარამეტრების შემოწმებულად მონიშვნა", "Mark as requiring review again": "ისევ შემოწმების საჭიროებად მონიშვნა",
  "Bulk edit parameters with Excel": "პარამეტრების ჯგუფური რედაქტირება Excel-ით",
  "Download a workbook with every asset and profile, fill in approved values (fuel, heat rate, fuel price, VOM, capacities, availability/inflow profiles, review status), and upload it. Empty cells leave values unchanged. You see every change before it is applied.":
    "ჩამოტვირთეთ ფაილი ყველა აქტივითა და პროფილით, შეავსეთ დამტკიცებული მნიშვნელობები (საწვავი, სპეციფიკური ხარჯი, საწვავის ფასი, VOM, სიმძლავრეები, პროფილები, შემოწმების სტატუსი) და ატვირთეთ. ცარიელი უჯრა არ ცვლის მნიშვნელობას. ყველა ცვლილებას გამოყენებამდე ნახავთ.",
  "Download Excel template": "Excel შაბლონის ჩამოტვირთვა", "Filled workbook (.xlsx)": "შევსებული ფაილი (.xlsx)",
  "Imported by": "იმპორტიორი", "Your name or initials (optional)": "თქვენი სახელი ან ინიციალები (არასავალდებულო)",
  "Check workbook": "ფაილის შემოწმება", "Checking...": "მოწმდება...", "Choose the filled .xlsx file first.": "ჯერ აირჩიეთ შევსებული .xlsx ფაილი.",
  "Changes": "ცვლილებები", "Asset": "აქტივი", "Current": "მიმდინარე", "New": "ახალი", "Row": "სტრიქონი", "Cell": "უჯრა",
  "Apply changes to project": "ცვლილებების გამოყენება პროექტზე",
  "Saves a new project revision and records the file hash and every change.": "ინახება პროექტის ახალი ვერსია, ფაილის ჰეში და ყველა ცვლილება.",
  // scenarios
  "A scenario is a deterministic overlay on the base project. Overlays cannot change project identity or version.":
    "სცენარი საბაზო პროექტზე დადებული ცვლილებაა. მას არ შეუძლია პროექტის იდენტიფიკატორის ან ვერსიის შეცვლა.",
  "No scenarios. Runs use the base case.": "სცენარები არ არის. გაშვება საბაზო ვარიანტზე ხდება.", "Overlay": "ცვლილება",
  "Delete": "წაშლა", "Add a scenario": "სცენარის დამატება", "Scenario id": "სცენარის id", "Overlay (JSON object)": "ცვლილება (JSON ობიექტი)",
  "Add scenario": "სცენარის დამატება", "e.g. high-demand": "მაგ. high-demand",
  "Mappings merge into the base project; lists and values replace it. Example: {\"solver_settings\": {\"mip_gap\": 0.01}}.":
    "ობიექტები საბაზო პროექტს ერწყმის, სიები და მნიშვნელობები კი ცვლის. მაგალითი: {\"solver_settings\": {\"mip_gap\": 0.01}}.",
  // validation
  "Contract errors block saving. Run-readiness errors block run creation. Warnings are reported for review.":
    "კონტრაქტის შეცდომები შენახვას ბლოკავს. გაშვების მზადყოფნის შეცდომები გაშვებას ბლოკავს. გაფრთხილებები შესამოწმებლად ჩანს.",
  "Validate working copy": "სამუშაო ასლის ვალიდაცია", "The current working copy has not been validated yet.": "სამუშაო ასლის ვალიდაცია ჯერ არ ჩატარებულა.",
  "Valid and run-ready": "ვალიდურია და მზადაა გასაშვებად", "Valid project, but not run-ready": "პროექტი ვალიდურია, მაგრამ გასაშვებად არ არის მზად",
  "Validation failed - the project cannot be saved": "ვალიდაცია ვერ გაიარა - პროექტის შენახვა შეუძლებელია",
  "Severity": "სიმძიმე", "Path": "გზა", "Code": "კოდი", "Message": "შეტყობინება", "Remediation": "გამოსწორება", "No issues.": "პრობლემები არ არის.", "Code: ": "კოდი: ",
  // workflow inputs
  "Workflow inputs": "ამოცანის შემავალი მონაცემები",
  "Inputs specific to the selected workflow. They are checked by the solver's own validator when you choose Validate; errors appear under Validation before any run is created.":
    "არჩეული ამოცანის სპეციფიკური მონაცემები. „ვალიდაციისას“ მათ ამომხსნელის ვალიდატორი ამოწმებს; შეცდომები ჩანს „ვალიდაციაში“ გაშვებამდე.",
  "Use the shared component engine (required by stochastic UC)": "გაზიარებული კომპონენტების ძრავის გამოყენება (სავალდებულოა სტოქასტური UC-სთვის)",
  "Use the shared component engine (required by N-1 security UC)": "გაზიარებული კომპონენტების ძრავის გამოყენება (სავალდებულოა N-1 UC-სთვის)",
  "Scenario id": "სცენარის id", "Probability": "ალბათობა", "Demand profile": "მოთხოვნის პროფილი", "(base demand)": "(საბაზო მოთხოვნა)",
  "Contingency id": "ავარიული შემთხვევის id", "Element": "ელემენტი", "(choose)": "(აირჩიეთ)", "Unit outage": "აგრეგატის გათიშვა",
  "Import outage": "იმპორტის გათიშვა", "Line outage": "ხაზის გათიშვა", "Add contingency": "ავარიული შემთხვევის დამატება",
  "Reserve products (required for corrective redispatch)": "რეზერვის პროდუქტები (სავალდებულოა მაკორექტირებელი გადანაწილებისთვის)",
  "Product id": "პროდუქტის id", "Direction": "მიმართულება", "Requirement (MW)": "მოთხოვნა (მგვტ)", "Eligible units": "დასაშვები აგრეგატები",
  "Add reserve product": "რეზერვის პროდუქტის დამატება", "Monte Carlo samples": "მონტე-კარლოს ნიმუშები", "Random seed": "შემთხვევითი რიცხვის საწყისი (seed)",
  "Forced outage rates": "იძულებითი გათიშვის ალბათობები", "Forced outage rate (0-1)": "იძულებითი გათიშვის ალბათობა (0-1)",
  "Planning years": "დაგეგმვის წლები", "Discount rate": "დისკონტის განაკვეთი", "Reserve margin": "რეზერვის მარჟა",
  "Screening only: a continuous LP for candidate sizing, not an investment decision model.":
    "მხოლოდ წინასწარი შეფასება: უწყვეტი LP კანდიდატების ზომისთვის, არა საინვესტიციო გადაწყვეტილების მოდელი.",
  "Candidate id": "კანდიდატის id", "CAPEX (/MW)": "CAPEX (/მგვტ)", "Fixed O&M (/MW-yr)": "ფიქსირებული O&M (/მგვტ-წელი)",
  "Max build (MW)": "მაქს. მშენებლობა (მგვტ)", "Life (years)": "ექსპლუატაციის ვადა (წელი)", "Capacity credit (0-1)": "სიმძლავრის კრედიტი (0-1)",
  "Capacity factor (0-1)": "სიმძლავრის გამოყენების კოეფიციენტი (0-1)", "Add candidate": "კანდიდატის დამატება",
  // runs
  "Scenario": "სცენარი", "Base case": "საბაზო ვარიანტი", "Create and launch run": "გაშვების შექმნა და დაწყება", "Ready to run.": "მზადაა გასაშვებად.",
  "Save your changes first: a run uses the saved project snapshot.": "ჯერ შეინახეთ ცვლილებები: გაშვება შენახული პროექტის ასლს იყენებს.",
  "The saved project has blocking validation errors (see Validation).": "შენახულ პროექტს აქვს ბლოკირების შეცდომები (იხ. ვალიდაცია).",
  "Showing all runs in the workspace. Open a project to create runs.": "ნაჩვენებია სამუშაო სივრცის ყველა გაშვება. გაშვების შესაქმნელად გახსენით პროექტი.",
  "No runs yet.": "გაშვებები ჯერ არ არის.", "Created": "შექმნილია", "Run": "გაშვება", "QA": "QA", "Publishable": "გამოქვეყნებადი",
  "Actions": "მოქმედებები", "Details": "დეტალები", "Export": "ექსპორტი", "yes": "კი", "no": "არა",
  "Sensitivity analysis": "მგრძნობიარობის ანალიზი",
  "Creates one scenario per factor that multiplies the chosen input (for example demand x0.9 and x1.1), saves the project and launches the runs. Compare them under Compare - Scenario overview.":
    "თითოეული კოეფიციენტისთვის იქმნება სცენარი, რომელიც არჩეულ მონაცემს ამრავლებს (მაგ. მოთხოვნა x0.9 და x1.1), პროექტი ინახება და გაშვებები იწყება. შედეგები: შედარება - სცენარების მიმოხილვა.",
  "Input to vary": "ცვლადი მონაცემი", "Starting from": "საწყისი სცენარი", "Factors": "კოეფიციენტები", "Create and run sensitivity": "მგრძნობიარობის შექმნა და გაშვება",
  "Creating scenarios and queuing runs...": "სცენარების შექმნა და გაშვებების რიგში ჩაყენება...",
  "Enter 1-9 factors between 0 and 10, for example 0.9, 1.1.": "შეიყვანეთ 1-9 კოეფიციენტი 0-დან 10-მდე, მაგ. 0.9, 1.1.",
  // results
  "No runs yet. Create a run on the Runs page.": "გაშვებები ჯერ არ არის. შექმენით „გაშვებების“ გვერდზე.", "Loading result...": "შედეგი იტვირთება...",
  "Run status": "გაშვების სტატუსი", "Publication": "გამოქვეყნება", "Workflow / scenario": "ამოცანა / სცენარი",
  "Not publishable": "არ არის გამოქვეყნებადი", "Publishable (with warnings)": "გამოქვეყნებადი (გაფრთხილებებით)", "No result": "შედეგი არ არის",
  "QA not available": "QA მიუწვდომელია", "Results withheld": "შედეგები დამალულია",
  "Unserved energy of zero does not make a result publishable when canonical QA or the publication gate rejects it.":
    "ნულოვანი მიუწოდებელი ენერგია შედეგს გამოქვეყნებადს არ ხდის, თუ QA ან გამოქვეყნების კონტროლი მას უარყოფს.",
  "QA passed with warnings. Review the QA checks before using these results.": "QA გავიდა გაფრთხილებებით. გამოყენებამდე გადახედეთ QA შემოწმებებს.",
  "Run failed": "გაშვება ჩავარდა", "Run cancelled": "გაშვება გაუქმდა", "Cause: ": "მიზეზი: ",
  "Solver diagnostics and reproducibility": "ამომხსნელის დიაგნოსტიკა და განმეორებადობა", "Check": "შემოწმება", "Max violation": "მაქს. დარღვევა",
  "Tolerance": "დასაშვები ცდომილება", "Export run bundle (ZIP)": "გაშვების პაკეტის ექსპორტი (ZIP)", "Excel report": "Excel ანგარიში",
  "Printable report (PDF via print)": "დასაბეჭდი ანგარიში (PDF ბეჭდვით)", "By resource": "რესურსების მიხედვით", "Resource": "რესურსი",
  "Type": "ტიპი", "Energy (MWh)": "ენერგია (მგვტ.სთ)", "Capacity factor (%)": "სიმძლავრის გამოყენების კოეფიციენტი (%)", "Gross cost": "მთლიანი ხარჯი",
  "Avg cost (/MWh)": "საშ. ხარჯი (/მგვტ.სთ)", "Starts": "ჩართვები", "Monthly summary": "თვიური შეჯამება", "Month": "თვე", "Cost": "ხარჯი",
  "Peak load (MW)": "პიკური დატვირთვა (მგვტ)", "Avg price": "საშ. ფასი", "Unserved (MWh)": "მიუწოდებელი (მგვტ.სთ)", "Curtailed (MWh)": "შეზღუდული (მგვტ.სთ)",
  "Reported metrics": "მაჩვენებლები", "Metric": "მაჩვენებელი", "Value": "მნიშვნელობა", "This workflow reported no scalar metrics.": "ამ ამოცანას მაჩვენებლები არ აქვს.",
  "Energy served": "მიწოდებული ენერგია", "Objective cost": "მიზნის ფუნქცია", "System cost": "სისტემის ხარჯი", "Average price": "საშუალო ფასი",
  "Peak load": "პიკური დატვირთვა", "Unserved energy": "მიუწოდებელი ენერგია", "Curtailment": "შეზღუდვა", "Gas use": "გაზის მოხმარება", "CO2": "CO2",
  "Monthly energy balance": "თვიური ენერგეტიკული ბალანსი", "Monthly energy balance table (MWh)": "თვიური ენერგეტიკული ბალანსის ცხრილი (მგვტ.სთ)",
  "Hydro": "ჰიდრო", "Reservoir hydro": "წყალსაცავიანი ჰესი", "Wind": "ქარი", "Solar": "მზე", "Thermal": "თბოგენერაცია", "TPP / import": "თესი / იმპორტი",
  "Battery (net)": "ბატარეა (ნეტო)", "Pumped hydro": "ჰიდროაკუმულირება", "Demand response": "მოთხოვნაზე რეაგირება", "Unserved": "მიუწოდებელი",
  "Curtailed": "შეზღუდული", "Demand": "მოთხოვნა", "Dispatch by technology": "დისპეტჩირება ტექნოლოგიების მიხედვით",
  "daily mean MW": "დღიური საშუალო მგვტ", "MW": "მგვტ", "System load and price": "სისტემის დატვირთვა და ფასი", "daily mean": "დღიური საშუალო",
  "hourly": "საათობრივი", "Load, MW (left axis)": "დატვირთვა, მგვტ (მარცხენა ღერძი)", "Price, USD/MWh (right axis)": "ფასი, USD/მგვტ.სთ (მარჯვენა ღერძი)",
  "Wind/solar curtailment": "ქარის/მზის შეზღუდვა", "Peak demand": "პიკური მოთხოვნა", "Average system price": "სისტემის საშუალო ფასი", "CO2 emissions": "CO2 ემისიები",
  "Calibration against measured data": "კალიბრაცია ფაქტობრივ მონაცემებთან",
  "Download the template for this run, enter measured energy (MWh) per hour and/or per month for demand, asset kinds or individual assets, and upload it. PowerSim reports model vs actual totals, bias, MAE, RMSE and correlation. The comparison does not change the model.":
    "ჩამოტვირთეთ ამ გაშვების შაბლონი, შეიყვანეთ ფაქტობრივი ენერგია (მგვტ.სთ) საათობრივად ან თვიურად მოთხოვნისთვის, აქტივების ტიპებისთვის ან ცალკეული აქტივებისთვის და ატვირთეთ. PowerSim აჩვენებს მოდელისა და ფაქტის ჯამებს, გადახრას, MAE, RMSE და კორელაციას. შედარება მოდელს არ ცვლის.",
  "Download calibration template": "კალიბრაციის შაბლონის ჩამოტვირთვა", "Workbook with measured data (.xlsx)": "ფაქტობრივი მონაცემების ფაილი (.xlsx)",
  "Compare with measured data": "ფაქტობრივ მონაცემებთან შედარება", "Comparing...": "შედარება...", "Choose the filled workbook first.": "ჯერ აირჩიეთ შევსებული ფაილი.",
  "Resolution": "დეტალიზაცია", "Series": "მწკრივი", "Points": "წერტილები", "Model (MWh)": "მოდელი (მგვტ.სთ)", "Measured (MWh)": "ფაქტი (მგვტ.სთ)",
  "Bias (%)": "გადახრა (%)", "MAE (MWh)": "MAE (მგვტ.სთ)", "RMSE (MWh)": "RMSE (მგვტ.სთ)", "Correlation": "კორელაცია",
  // compare
  "Compare runs": "გაშვებების შედარება", "Scenario overview": "სცენარების მიმოხილვა", "Loading...": "იტვირთება...",
  "No completed publishable runs yet.": "დასრულებული გამოქვეყნებადი გაშვებები ჯერ არ არის.", "Latest completed run per scenario.": "თითოეული სცენარის ბოლო დასრულებული გაშვება.",
  "Compare two runs": "ორი გაშვების შედარება",
  "Only completed runs with persisted results can be compared. Metrics are shown only when both runs are publishable and use the same workflow.":
    "შედარება შესაძლებელია მხოლოდ დასრულებული გაშვებების. მაჩვენებლები ჩანს, თუ ორივე გამოქვეყნებადია და ერთი ამოცანისაა.",
  "At least two completed runs are needed for a comparison.": "შედარებისთვის საჭიროა მინიმუმ ორი დასრულებული გაშვება.",
  "Baseline run (left)": "საბაზო გაშვება (მარცხნივ)", "Comparison run (right)": "შესადარებელი გაშვება (მარჯვნივ)",
  "Compatible runs": "თავსებადი გაშვებები", "Not directly comparable": "პირდაპირ არ შედარდება", "Item": "პუნქტი", "Left": "მარცხნივ", "Right": "მარჯვნივ",
  "Difference (right - left)": "სხვაობა (მარჯვნივ - მარცხნივ)", "Neither run emitted comparable canonical metrics.": "არცერთ გაშვებას შესადარებელი მაჩვენებლები არ აქვს.",
  // reference data
  "Reference catalogue - requires user review before use in a study": "საცნობარო კატალოგი - კვლევაში გამოყენებამდე შემოწმებას საჭიროებს",
  "Loading reference catalogue...": "საცნობარო კატალოგი იტვირთება...", "Stations": "სადგურები", "Reservoirs": "წყალსაცავები", "Hydropower": "ჰიდროსადგურები",
  "HPP summary": "ჰესების შეჯამება", "Quality report": "ხარისხის ანგარიში", "Reference tables": "საცნობარო ცხრილები",
  "Search name, river, region...": "ძებნა: სახელი, მდინარე, რეგიონი...", "Search the reference table": "საცნობარო ცხრილში ძებნა",
  "No records match the search.": "ძებნის შედეგი არ არის.", "Stage": "მონიშვნა", "Quality": "ხარისხი",
  "Section headings and totals cannot be imported": "განყოფილების სათაურები და ჯამები არ შემოიტანება",
  "Severity": "სიმძიმე", "Table": "ცხრილი", "Finding": "აღმოჩენა", "Findings": "აღმოჩენები", "Close": "დახურვა",
  "Quality findings": "ხარისხის აღმოჩენები", "Records in other tables with similar names": "მსგავსი სახელის ჩანაწერები სხვა ცხრილებში", "No matches.": "დამთხვევა არ არის.",
  "Stage records into a project": "ჩანაწერების პროექტში შემოტანა",
  "No records staged. Tick plant rows in the Stations table to stage them.": "ჩანაწერები არ არის მონიშნული. მონიშნეთ სადგურები „სადგურების“ ცხრილში.",
  "Review import mapping": "იმპორტის შესაბამისობის შემოწმება", "Clear staged records": "მონიშვნის გასუფთავება",
  "PowerSim field": "PowerSim-ის ველი", "Source cell": "წყაროს უჯრა", "Note": "შენიშვნა", "Issues": "პრობლემები",
  "Manual overrides (recorded in provenance)": "ხელით შესწორებები (ფიქსირდება წარმომავლობაში)", "Override value": "შესწორებული მნიშვნელობა",
  "Reason": "მიზეზი", "Reason and source (required)": "მიზეზი და წყარო (სავალდებულო)", "(no override)": "(შესწორების გარეშე)",
  "Component kind": "კომპონენტის ტიპი", "Review again with overrides": "ხელახლა შემოწმება შესწორებებით", "Records": "ჩანაწერები", "Overrides": "შესწორებები",
  "Imported at": "შემოტანის დრო", "Workbook SHA-256": "ფაილის SHA-256",
  // about
  "Capabilities and limits": "შესაძლებლობები და შეზღუდვები", "Validated workflows": "ვალიდირებული ამოცანები", "Data boundaries": "მონაცემების საზღვრები",
  "Backend information is unavailable.": "სერვერის ინფორმაცია მიუწვდომელია.",
  "The Georgia demonstration is sanitized and aggregated. It is not official GSE operational data and not an official dispatch case.":
    "საქართველოს სადემონსტრაციო პროექტი გაერთიანებული და ანონიმიზებულია. ეს არ არის სსე-ს ოფიციალური საოპერაციო მონაცემები ან ოფიციალური დისპეტჩერული შემთხვევა.",
  "The plant-parameter reference catalogue is reference data. Its values must be reviewed against approved sources before they are used in a study; imported assets stay disabled until a user marks them reviewed.":
    "სადგურების პარამეტრების კატალოგი საცნობარო მონაცემებია. კვლევაში გამოყენებამდე ისინი დამტკიცებულ წყაროებთან უნდა შემოწმდეს; შემოტანილი აქტივები გამორთულია, სანამ მომხმარებელი შემოწმებულად არ მონიშნავს.",
  "Results are shown only when the run's result is valid and passes canonical QA and the publication gate.":
    "შედეგები ჩანს მხოლოდ მაშინ, როცა ის ვალიდურია და გაიარა QA და გამოქვეყნების კონტროლი.",
  "See docs/POWERSIM_1_0_CAPABILITIES.md, docs/POWERSIM_1_0_LIMITATIONS.md and docs/POWERSIM_1_0_USER_GUIDE.md in the distribution.":
    "იხილეთ docs/POWERSIM_1_0_CAPABILITIES.md, docs/POWERSIM_1_0_LIMITATIONS.md და docs/POWERSIM_1_0_USER_GUIDE.md.",
  // status words (badges)
  "draft": "მონახაზი", "validating": "ვალიდაცია", "queued": "რიგში", "preparing": "მომზადება", "solving": "ამოხსნა",
  "validating_results": "შედეგების შემოწმება", "reporting": "ანგარიში", "completed": "დასრულდა", "failed": "ჩავარდა", "cancelled": "გაუქმდა",
  "pass": "გავიდა", "fail": "ვერ გავიდა", "warn": "გაფრთხილება", "error": "შეცდომა", "warning": "გაფრთხილება", "info": "ინფო", "resolved": "გადაწყვეტილი",
};

const DICTIONARIES = { ka: KA };
const STORAGE_KEY = "powersim.language";
let language = "en";
try {
  const stored = window.localStorage.getItem(STORAGE_KEY);
  if (stored === "ka" || stored === "en") language = stored;
} catch { /* storage unavailable: keep English */ }

export function getLanguage() { return language; }
export function setLanguage(next) {
  language = next === "ka" ? "ka" : "en";
  try { window.localStorage.setItem(STORAGE_KEY, language); } catch { /* ignore */ }
  document.documentElement.lang = language;
}
export function translate(text) {
  if (language === "en" || typeof text !== "string") return text;
  const table = DICTIONARIES[language];
  return Object.prototype.hasOwnProperty.call(table, text) ? table[text] : text;
}
export function translations(lang) { return DICTIONARIES[lang] || {}; }
