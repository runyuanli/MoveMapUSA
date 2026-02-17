County Price Filter Map (VSCode Java project)

What this is
- A minimal Java HTTP server (no frameworks) that serves:
  - a static web page with a USA county map + slider
  - a local counties TopoJSON file (you download once)
  - a local, human-editable price file (TSV) that the page reads

Requirements
- Java 17+ installed
- Any modern web browser

Project layout
- src/main/java/com/rich/countyfilter/Main.java   (server)
- src/main/resources/public/index.html           (frontend)
- data/prices.tsv                                 (edit this)
- data/counties-10m.json                           (you download this)

1) Download the county geometry file (one-time)
This project expects a TopoJSON file at:
  data/counties-10m.json

Download it from the us-atlas project:
  https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json

Save it as:
  data/counties-10m.json

2) Edit prices
Open:
  data/prices.tsv

Format (tab-separated):
  fips<TAB>median_sale_price
Example:
  06075   1350000

FIPS must be 5 digits (leading zeros kept).

3) Build (javac)
From the project root:
  mkdir -p out
  javac -d out src/main/java/com/rich/countyfilter/Main.java

4) Run (java)
  java -cp out com.rich.countyfilter.Main

Then open:
  http://localhost:8080

Notes
- The slider goes from 0 to 3,000,000.
- Counties with median sale price <= slider are highlighted in green.
- If a county is missing from prices.tsv, it is treated as "unknown" and shown in a neutral color.

Troubleshooting
- If you see a blank map, ensure data/counties-10m.json exists and is valid JSON.
- If you see all-neutral counties, check prices.tsv formatting (tab-separated, 5-digit FIPS).
