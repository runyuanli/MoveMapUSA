# County Price Filter Map

This project includes a Java generator for `data/prices.tsv` using Redfin's county market tracker dataset (small and fast), not the large weekly dataset.

## Generate `data/prices.tsv` (Windows PowerShell)

```powershell
mkdir out -ea 0
javac -d out src\main\java\com\rich\countyfilter\Main.java src\main\java\com\rich\countyfilter\GeneratePrices.java
java -cp out com.rich.countyfilter.GeneratePrices
```

Output format:

```text
fips	median_sale_price
01001	215000
```

## Run server

```powershell
java -cp out com.rich.countyfilter.Main
```

## Publish on GitHub Pages (free)

This repo includes a ready `docs/` folder for static hosting.

1. Push your latest commit to `main`.
2. In GitHub: `Settings` -> `Pages`.
3. Under `Build and deployment`:
   - `Source`: `Deploy from a branch`
   - `Branch`: `main`
   - `Folder`: `/docs`
4. Save, then wait ~1 minute.

Your site URL will be:

```text
https://runyuanli.github.io/MoveMapUSA/
```
