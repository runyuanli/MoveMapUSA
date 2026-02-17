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
