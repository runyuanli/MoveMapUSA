package com.rich.countyfilter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.Normalizer;
import java.time.Duration;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

/**
 * Generates data/prices.tsv from Redfin county market tracker data.
 */
public class GeneratePrices {

    private static final String REDFIN_COUNTY_TRACKER_GZ =
            "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/county_market_tracker.tsv000.gz";
    private static final String CENSUS_COUNTY_CODES =
            "https://www2.census.gov/geo/docs/reference/codes2020/national_county2020.txt";

    public static void main(String[] args) throws Exception {
        Path out = Path.of("data/prices.tsv");
        Files.createDirectories(out.getParent());

        System.out.println("Downloading and parsing Redfin county market tracker...");
        Map<String, CountyRecord> latestByFips = loadLatestCountyPrices();

        System.out.println("Writing prices.tsv...");
        try (BufferedWriter w = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {
            w.write("fips\tmedian_sale_price\n");

            List<Map.Entry<String, CountyRecord>> rows = new ArrayList<>(latestByFips.entrySet());
            rows.sort(Comparator.comparing(Map.Entry::getKey));
            for (Map.Entry<String, CountyRecord> e : rows) {
                w.write(e.getKey());
                w.write("\t");
                w.write(Long.toString(e.getValue().medianSalePrice));
                w.write("\n");
            }
        }

        System.out.println("Wrote: " + out.toAbsolutePath());
        System.out.println("County rows: " + latestByFips.size());
    }

    private static Map<String, CountyRecord> loadLatestCountyPrices() throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(REDFIN_COUNTY_TRACKER_GZ))
                .timeout(Duration.ofMinutes(5))
                .GET()
                .build();

        HttpResponse<InputStream> resp = client.send(req, HttpResponse.BodyHandlers.ofInputStream());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new IOException("Failed to download Redfin county market tracker: HTTP " + resp.statusCode());
        }

        try (GZIPInputStream gis = new GZIPInputStream(resp.body());
             BufferedReader r = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8))) {

            String header = r.readLine();
            if (header == null) throw new IOException("Empty Redfin county market tracker file");

            String[] cols = header.split("\\t", -1);
            Map<String, Integer> idx = new HashMap<>();
            for (int i = 0; i < cols.length; i++) {
                idx.put(normalizeHeader(cols[i]), i);
            }

            int iPeriod = findPeriodColumn(idx);
            int iPrice = findFirstExisting(idx, "MEDIAN_SALE_PRICE");
            int iFips = findFirstExisting(idx, "REGION_FIPS", "REGION_FIPS_CODE", "FIPS", "COUNTY_FIPS", "GEOID");
            int iCounty = findFirstExisting(idx, "REGION", "REGION_NAME", "COUNTY", "COUNTY_NAME");
            int iState = findFirstExisting(idx, "STATE", "STATE_CODE", "STATE_ABBR", "STATE_NAME");

            if (iPeriod < 0) throw new IOException("Missing period column (expected PERIOD_END or similar)");
            if (iPrice < 0) throw new IOException("Missing required column: MEDIAN_SALE_PRICE");
            if (iFips < 0 && (iCounty < 0 || iState < 0)) {
                throw new IOException("Missing county/state columns for FIPS fallback mapping");
            }

            Map<String, String> countyStateToFips = null;
            if (iFips < 0) {
                System.out.println("No county FIPS column found; downloading Census county codes for fallback mapping...");
                countyStateToFips = loadCountyFipsByStateCounty();
            }

            Map<String, CountyRecord> latestByFips = new HashMap<>();
            Set<String> loggedUnmatched = new HashSet<>();
            long processed = 0;
            long skipped = 0;
            String line;
            while ((line = r.readLine()) != null) {
                processed++;
                String[] parts = line.split("\\t", -1);

                if (parts.length <= Math.max(iPeriod, iPrice)) {
                    skipped++;
                    continue;
                }

                String periodRaw = valueAt(parts, iPeriod);
                ComparableDate period = parseComparableDate(periodRaw);
                if (period == null) {
                    skipped++;
                    continue;
                }

                String priceRaw = valueAt(parts, iPrice);
                Long price = parsePrice(priceRaw);
                if (price == null) {
                    skipped++;
                    continue;
                }

                String fips;
                if (iFips >= 0) {
                    fips = normalizeFips(valueAt(parts, iFips));
                    if (fips == null) {
                        skipped++;
                        continue;
                    }
                } else {
                    if (parts.length <= Math.max(iCounty, iState)) {
                        skipped++;
                        continue;
                    }
                    String countyName = valueAt(parts, iCounty);
                    String stateValue = valueAt(parts, iState);
                    String key = countyStateKey(countyName, stateValue);
                    if (key == null) {
                        skipped++;
                        continue;
                    }
                    fips = countyStateToFips.get(key);
                    if (fips == null) {
                        if (loggedUnmatched.add(key)) {
                            System.err.println("Skipping unmatched county/state: county='" + countyName + "', state='" + stateValue + "'");
                        }
                        skipped++;
                        continue;
                    }
                }

                CountyRecord existing = latestByFips.get(fips);
                if (existing == null || period.compareTo(existing.period) > 0) {
                    latestByFips.put(fips, new CountyRecord(period, price));
                }
            }

            System.out.println("Processed rows: " + processed);
            System.out.println("Skipped rows: " + skipped);
            return latestByFips;
        }
    }

    private static Map<String, String> loadCountyFipsByStateCounty() throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(CENSUS_COUNTY_CODES))
                .timeout(Duration.ofMinutes(2))
                .GET()
                .build();

        HttpResponse<InputStream> resp = client.send(req, HttpResponse.BodyHandlers.ofInputStream());
        if (resp.statusCode() < 200 || resp.statusCode() >= 300) {
            throw new IOException("Failed to download Census county codes: HTTP " + resp.statusCode());
        }

        Map<String, String> map = new HashMap<>();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(resp.body(), StandardCharsets.UTF_8))) {
            String header = r.readLine();
            if (header == null || !header.startsWith("STATE|STATEFP|COUNTYFP")) {
                throw new IOException("Unexpected Census county codes format");
            }

            String line;
            while ((line = r.readLine()) != null) {
                String[] parts = line.split("\\|");
                if (parts.length < 5) continue;

                String stateAbbr = parts[0].trim().toUpperCase(Locale.ROOT);
                String statefp = parts[1].trim();
                String countyfp = parts[2].trim();
                String countyName = parts[4].trim();
                String fips = statefp + countyfp;

                if (fips.length() != 5) continue;
                String key = countyStateKey(countyName, stateAbbr);
                if (key != null) {
                    map.put(key, fips);
                    String alias = cityAliasKey(key);
                    if (alias != null) map.putIfAbsent(alias, fips);
                }
            }
        }
        return map;
    }

    private static int findPeriodColumn(Map<String, Integer> idx) {
        int i = findFirstExisting(idx, "PERIOD_END", "PERIOD_BEGIN");
        if (i >= 0) return i;
        for (Map.Entry<String, Integer> e : idx.entrySet()) {
            String k = e.getKey();
            if (k.contains("PERIOD") && k.contains("END")) return e.getValue();
        }
        return -1;
    }

    private static int findFirstExisting(Map<String, Integer> idx, String... names) {
        for (String name : names) {
            Integer i = idx.get(name);
            if (i != null) return i;
        }
        return -1;
    }

    private static String valueAt(String[] parts, int index) {
        if (index < 0 || index >= parts.length) return "";
        return stripQuotes(parts[index]).trim();
    }

    private static String normalizeHeader(String s) {
        return stripQuotes(s).trim().toUpperCase(Locale.ROOT);
    }

    private static String stripQuotes(String s) {
        if (s == null) return "";
        String t = s.trim();
        if (t.length() >= 2 && t.startsWith("\"") && t.endsWith("\"")) {
            return t.substring(1, t.length() - 1).trim();
        }
        return t;
    }

    private static Long parsePrice(String raw) {
        String t = stripQuotes(raw).trim();
        if (t.isEmpty() || "NA".equalsIgnoreCase(t)) return null;
        try {
            double d = Double.parseDouble(t);
            if (!Double.isFinite(d)) return null;
            return Math.round(d);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static String normalizeFips(String raw) {
        String t = stripQuotes(raw).trim();
        if (t.isEmpty() || "NA".equalsIgnoreCase(t)) return null;

        StringBuilder digits = new StringBuilder();
        for (int i = 0; i < t.length(); i++) {
            char ch = t.charAt(i);
            if (Character.isDigit(ch)) digits.append(ch);
        }

        if (digits.length() == 0) return null;
        if (digits.length() > 5) return null;
        while (digits.length() < 5) digits.insert(0, '0');
        return digits.toString();
    }

    private static String countyStateKey(String countyRaw, String stateRaw) {
        String county = normalizeCountyName(countyRaw);
        if (county.isEmpty()) return null;
        String state = normalizeState(stateRaw);
        if (state.isEmpty()) return null;
        return state + "|" + county;
    }

    private static String cityAliasKey(String key) {
        int p = key.indexOf('|');
        if (p < 0 || p + 1 >= key.length()) return null;
        String state = key.substring(0, p);
        String county = key.substring(p + 1);
        if (!county.endsWith(" CITY")) return null;
        return state + "|" + county.substring(0, county.length() - 5).trim();
    }

    private static String normalizeCountyName(String countyRaw) {
        String n = stripQuotes(countyRaw);
        int comma = n.indexOf(',');
        if (comma >= 0) n = n.substring(0, comma);
        n = n.trim();
        n = Normalizer.normalize(n, Normalizer.Form.NFD).replaceAll("\\p{M}+", "");
        n = n.replace("&", " and ");
        n = n.replace("St. ", "Saint ");
        n = n.replace("Ste. ", "Sainte ");
        n = n.replace(" Parish", " County");
        n = n.replace(" Borough", " County");
        n = n.replace(" Census Area", " County");
        n = n.replace(" City and Borough", " County");
        n = n.replace(" City County", " City");
        n = n.replaceAll("\\bCounty County\\b", "County");
        n = n.replaceAll("\\s+", " ");
        return n.toUpperCase(Locale.ROOT);
    }

    private static String normalizeState(String stateRaw) {
        String s = stripQuotes(stateRaw).trim();
        if (s.isEmpty()) return "";
        s = s.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
        if (s.length() == 2) return s;
        String abbr = STATE_NAME_TO_ABBR.get(s);
        return abbr == null ? "" : abbr;
    }

    private static ComparableDate parseComparableDate(String raw) {
        String t = stripQuotes(raw).trim();
        if (t.isEmpty()) return null;
        try {
            return ComparableDate.forLocalDate(LocalDate.parse(t));
        } catch (DateTimeParseException ignored) {
            if (t.matches("\\d{4}-\\d{2}-\\d{2}")) {
                return ComparableDate.forIsoString(t);
            }
            System.err.println("Skipping non-ISO period value: '" + t + "'");
            return null;
        }
    }

    private static class ComparableDate implements Comparable<ComparableDate> {
        final LocalDate date;
        final String isoText;

        private ComparableDate(LocalDate date, String isoText) {
            this.date = date;
            this.isoText = isoText;
        }

        static ComparableDate forLocalDate(LocalDate d) {
            return new ComparableDate(d, null);
        }

        static ComparableDate forIsoString(String s) {
            return new ComparableDate(null, s);
        }

        @Override
        public int compareTo(ComparableDate other) {
            if (this.date != null && other.date != null) return this.date.compareTo(other.date);
            if (this.date != null) return this.date.toString().compareTo(other.isoText);
            if (other.date != null) return this.isoText.compareTo(other.date.toString());
            return this.isoText.compareTo(other.isoText);
        }
    }

    private static class CountyRecord {
        final ComparableDate period;
        final long medianSalePrice;

        CountyRecord(ComparableDate period, long medianSalePrice) {
            this.period = period;
            this.medianSalePrice = medianSalePrice;
        }
    }

    private static final Map<String, String> STATE_NAME_TO_ABBR = Map.ofEntries(
            Map.entry("ALABAMA", "AL"),
            Map.entry("ALASKA", "AK"),
            Map.entry("ARIZONA", "AZ"),
            Map.entry("ARKANSAS", "AR"),
            Map.entry("CALIFORNIA", "CA"),
            Map.entry("COLORADO", "CO"),
            Map.entry("CONNECTICUT", "CT"),
            Map.entry("DELAWARE", "DE"),
            Map.entry("DISTRICT OF COLUMBIA", "DC"),
            Map.entry("FLORIDA", "FL"),
            Map.entry("GEORGIA", "GA"),
            Map.entry("HAWAII", "HI"),
            Map.entry("IDAHO", "ID"),
            Map.entry("ILLINOIS", "IL"),
            Map.entry("INDIANA", "IN"),
            Map.entry("IOWA", "IA"),
            Map.entry("KANSAS", "KS"),
            Map.entry("KENTUCKY", "KY"),
            Map.entry("LOUISIANA", "LA"),
            Map.entry("MAINE", "ME"),
            Map.entry("MARYLAND", "MD"),
            Map.entry("MASSACHUSETTS", "MA"),
            Map.entry("MICHIGAN", "MI"),
            Map.entry("MINNESOTA", "MN"),
            Map.entry("MISSISSIPPI", "MS"),
            Map.entry("MISSOURI", "MO"),
            Map.entry("MONTANA", "MT"),
            Map.entry("NEBRASKA", "NE"),
            Map.entry("NEVADA", "NV"),
            Map.entry("NEW HAMPSHIRE", "NH"),
            Map.entry("NEW JERSEY", "NJ"),
            Map.entry("NEW MEXICO", "NM"),
            Map.entry("NEW YORK", "NY"),
            Map.entry("NORTH CAROLINA", "NC"),
            Map.entry("NORTH DAKOTA", "ND"),
            Map.entry("OHIO", "OH"),
            Map.entry("OKLAHOMA", "OK"),
            Map.entry("OREGON", "OR"),
            Map.entry("PENNSYLVANIA", "PA"),
            Map.entry("RHODE ISLAND", "RI"),
            Map.entry("SOUTH CAROLINA", "SC"),
            Map.entry("SOUTH DAKOTA", "SD"),
            Map.entry("TENNESSEE", "TN"),
            Map.entry("TEXAS", "TX"),
            Map.entry("UTAH", "UT"),
            Map.entry("VERMONT", "VT"),
            Map.entry("VIRGINIA", "VA"),
            Map.entry("WASHINGTON", "WA"),
            Map.entry("WEST VIRGINIA", "WV"),
            Map.entry("WISCONSIN", "WI"),
            Map.entry("WYOMING", "WY"),
            Map.entry("PUERTO RICO", "PR")
    );
}
