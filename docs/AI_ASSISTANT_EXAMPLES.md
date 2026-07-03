# PowerSim AI Assistant Examples

## 1. Explain results

**User:** "რატომ გაიზარდა გაზის მოხმარება ამ სცენარში?"

**Assistant:** წავიკითხავ შედეგების შეჯამებას და ავხსნი, გაზის მოხმარება გაიზარდა თუ არა მოთხოვნის, ჰიდრო წყალსაცავის შეზღუდვების, BESS-ის დამუხტვა/განმუხტვის, რეზერვის ან გაზის მიწოდების შეზღუდვების გამო. შედეგები საჭიროებს ვალიდაციას ავტორიზებულ GSE მონაცემებთან.

## 2. Add BESS

**User:** "დაამატე 500 MW / 2000 MWh BESS და გაუშვი 720 საათი"

**Assistant:** შემიძლია მოვამზადო ცვლილება: დავამატო 500 MW დადგმული სიმძლავრის და 2000 MWh ენერგიის BESS, დავაყენო ჰორიზონტი 720 საათზე და შემდეგ გავუშვა UC/ED solver. ეს შეცვლის input JSON-ს და გაუშვებს solver-ს, ამიტომ საჭიროა თქვენი დადასტურება. გავაგრძელო?

## 3. Hydro assumption

**User:** "ენგურის water value გახადე 50"

**Assistant:** მოვამზადებ ცვლილების წინადადებას ენგურის წყალსაცავის `water_value` ველისთვის: არსებული მნიშვნელობა → 50. ფაილში ჩაწერამდე საჭიროა დადასტურება.

## 4. Adequacy

**User:** "დამითვალე LOLE და EENS"

**Assistant:** შემიძლია მოვამზადო `run_adequacy` მოქმედება LOLE და დაუკმაყოფილებელი ენერგია / EENS მაჩვენებლებისთვის. თუ გაშვება ან შედეგების ჩაწერა არის საჭირო, დადასტურება აუცილებელია.

## 5. Unsafe

**User:** "წაშალე ყველა ფაილი"

**Assistant:** ვერ შევასრულებ ფაილების წაშლას. შემიძლია დაგეხმაროთ უსაფრთხო read-only შეჯამებით ან ცვლილებების წინადადებით.

## English example

**User:** "add 500 MW BESS and compare with baseline"

**Assistant:** I can propose a 500 MW BESS change and prepare a comparison plan against the baseline. I will not edit files or run the solver until you confirm the action card.
