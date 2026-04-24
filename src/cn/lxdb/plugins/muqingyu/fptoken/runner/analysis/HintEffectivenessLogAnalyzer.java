package cn.lxdb.plugins.muqingyu.fptoken.runner.analysis;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Offline analyzer for merge performance logs.
 *
 * <p>It classifies each observed run into effective / neutral / negative optimization buckets and
 * computes ratios for hint enablement decisions.</p>
 */
public final class HintEffectivenessLogAnalyzer {
    private static final Pattern KV = Pattern.compile("([A-Za-z][A-Za-z0-9]*)=([^,\\s]+)");

    private HintEffectivenessLogAnalyzer() {
    }

    public static void main(String[] args) throws Exception {
        CliArgs cli = CliArgs.parse(args);
        AnalysisSummary summary = analyzeFiles(cli.logFiles, cli.policy);
        printSummary(summary, cli.policy);
    }

    public static AnalysisSummary analyzeFiles(List<Path> logFiles, DecisionPolicy policy) throws IOException {
        List<Observation> observations = new ArrayList<>();
        for (int i = 0; i < logFiles.size(); i++) {
            Path f = logFiles.get(i);
            List<String> lines = Files.readAllLines(f, StandardCharsets.UTF_8);
            for (int lineNo = 0; lineNo < lines.size(); lineNo++) {
                Observation obs = parseObservationLine(lines.get(lineNo), f.toString(), lineNo + 1);
                if (obs != null) {
                    observations.add(obs);
                }
            }
        }
        return summarize(observations, policy);
    }

    public static Observation parseObservationLine(String line, String source, int lineNo) {
        if (line == null || line.indexOf('=') < 0) {
            return null;
        }
        Matcher m = KV.matcher(line);
        Map<String, String> kv = new HashMap<>();
        while (m.find()) {
            kv.put(m.group(1), m.group(2));
        }
        String improvement = firstNonEmpty(kv, "improveMedianPercent", "improvementPercent");
        if (improvement == null) {
            return null;
        }
        Double pct = parseDoubleSafe(improvement);
        if (pct == null) {
            return null;
        }
        String mode = firstNonEmpty(kv, "mode");
        if (mode == null) {
            mode = "unknown";
        }
        return new Observation(mode, pct.doubleValue(), source + ":" + lineNo);
    }

    public static AnalysisSummary summarize(List<Observation> observations, DecisionPolicy policy) {
        EnumMap<EffectClass, Integer> totals = new EnumMap<>(EffectClass.class);
        Map<String, EnumMap<EffectClass, Integer>> byMode = new HashMap<>();
        for (EffectClass c : EffectClass.values()) {
            totals.put(c, Integer.valueOf(0));
        }
        for (int i = 0; i < observations.size(); i++) {
            Observation o = observations.get(i);
            EffectClass cls = classify(o.improvementPercent, policy);
            totals.put(cls, Integer.valueOf(totals.get(cls).intValue() + 1));
            EnumMap<EffectClass, Integer> modeMap = byMode.get(o.mode);
            if (modeMap == null) {
                modeMap = new EnumMap<>(EffectClass.class);
                for (EffectClass c : EffectClass.values()) {
                    modeMap.put(c, Integer.valueOf(0));
                }
                byMode.put(o.mode, modeMap);
            }
            modeMap.put(cls, Integer.valueOf(modeMap.get(cls).intValue() + 1));
        }
        int total = observations.size();
        double effectiveRatio = ratio(totals.get(EffectClass.EFFECTIVE).intValue(), total);
        double negativeRatio = ratio(totals.get(EffectClass.NEGATIVE).intValue(), total);
        double neutralRatio = ratio(totals.get(EffectClass.NEUTRAL).intValue(), total);
        Decision decision = decide(effectiveRatio, negativeRatio, policy);
        return new AnalysisSummary(
                total,
                totals,
                Collections.unmodifiableMap(byMode),
                effectiveRatio,
                neutralRatio,
                negativeRatio,
                decision,
                observations
        );
    }

    public static EffectClass classify(double improvementPercent, DecisionPolicy policy) {
        if (improvementPercent >= policy.effectiveMinPercent) {
            return EffectClass.EFFECTIVE;
        }
        if (improvementPercent <= policy.negativeMaxPercent) {
            return EffectClass.NEGATIVE;
        }
        return EffectClass.NEUTRAL;
    }

    public static Decision decide(double effectiveRatio, double negativeRatio, DecisionPolicy policy) {
        if (negativeRatio >= policy.disableIfNegativeRatioAtLeast) {
            return Decision.DISABLE_HINTS_BY_DEFAULT;
        }
        if (effectiveRatio >= policy.enableIfEffectiveRatioAtLeast
                && negativeRatio <= policy.disableIfNegativeRatioAtLeast * 0.5d) {
            return Decision.ENABLE_HINTS_BY_DEFAULT;
        }
        return Decision.ENABLE_CONDITIONALLY_WITH_GUARDRAIL;
    }

    private static void printSummary(AnalysisSummary summary, DecisionPolicy policy) {
        System.out.println("=== Hint Effectiveness Offline Analysis ===");
        System.out.println("totalObservations=" + summary.totalObservations);
        System.out.println("effectiveRatio=" + formatPct(summary.effectiveRatio)
                + ", neutralRatio=" + formatPct(summary.neutralRatio)
                + ", negativeRatio=" + formatPct(summary.negativeRatio));
        System.out.println("policy: effectiveMinPercent=" + policy.effectiveMinPercent
                + ", negativeMaxPercent=" + policy.negativeMaxPercent
                + ", enableIfEffectiveRatioAtLeast=" + policy.enableIfEffectiveRatioAtLeast
                + ", disableIfNegativeRatioAtLeast=" + policy.disableIfNegativeRatioAtLeast);
        System.out.println("decision=" + summary.decision);
        for (Map.Entry<String, EnumMap<EffectClass, Integer>> e : summary.countsByMode.entrySet()) {
            EnumMap<EffectClass, Integer> m = e.getValue();
            int total = m.get(EffectClass.EFFECTIVE).intValue()
                    + m.get(EffectClass.NEUTRAL).intValue()
                    + m.get(EffectClass.NEGATIVE).intValue();
            System.out.println("mode=" + e.getKey()
                    + ", total=" + total
                    + ", effective=" + m.get(EffectClass.EFFECTIVE)
                    + ", neutral=" + m.get(EffectClass.NEUTRAL)
                    + ", negative=" + m.get(EffectClass.NEGATIVE));
        }
    }

    private static String formatPct(double ratio) {
        return String.format(Locale.ROOT, "%.2f%%", ratio * 100.0d);
    }

    private static double ratio(int v, int total) {
        if (total <= 0) {
            return 0.0d;
        }
        return v / (double) total;
    }

    private static Double parseDoubleSafe(String s) {
        try {
            return Double.valueOf(s);
        } catch (NumberFormatException ignore) {
            return null;
        }
    }

    private static String firstNonEmpty(Map<String, String> kv, String... keys) {
        for (int i = 0; i < keys.length; i++) {
            String v = kv.get(keys[i]);
            if (v != null && !v.isEmpty()) {
                return v;
            }
        }
        return null;
    }

    static final class CliArgs {
        private final List<Path> logFiles;
        private final DecisionPolicy policy;

        private CliArgs(List<Path> logFiles, DecisionPolicy policy) {
            this.logFiles = logFiles;
            this.policy = policy;
        }

        static CliArgs parse(String[] args) {
            List<Path> logFiles = new ArrayList<>();
            DecisionPolicy policy = DecisionPolicy.defaults();
            for (int i = 0; i < args.length; i++) {
                String a = args[i];
                if (a.startsWith("--")) {
                    policy = policy.withOverride(a);
                } else {
                    logFiles.add(Paths.get(a));
                }
            }
            if (logFiles.isEmpty()) {
                throw new IllegalArgumentException("usage: HintEffectivenessLogAnalyzer <logFile...> [--key=value]");
            }
            return new CliArgs(logFiles, policy);
        }
    }

    public enum EffectClass {
        EFFECTIVE,
        NEUTRAL,
        NEGATIVE
    }

    public enum Decision {
        ENABLE_HINTS_BY_DEFAULT,
        ENABLE_CONDITIONALLY_WITH_GUARDRAIL,
        DISABLE_HINTS_BY_DEFAULT
    }

    public static final class Observation {
        public final String mode;
        public final double improvementPercent;
        public final String source;

        Observation(String mode, double improvementPercent, String source) {
            this.mode = mode;
            this.improvementPercent = improvementPercent;
            this.source = source;
        }
    }

    public static final class AnalysisSummary {
        public final int totalObservations;
        public final EnumMap<EffectClass, Integer> totalCounts;
        public final Map<String, EnumMap<EffectClass, Integer>> countsByMode;
        public final double effectiveRatio;
        public final double neutralRatio;
        public final double negativeRatio;
        public final Decision decision;
        public final List<Observation> observations;

        AnalysisSummary(
                int totalObservations,
                EnumMap<EffectClass, Integer> totalCounts,
                Map<String, EnumMap<EffectClass, Integer>> countsByMode,
                double effectiveRatio,
                double neutralRatio,
                double negativeRatio,
                Decision decision,
                List<Observation> observations
        ) {
            this.totalObservations = totalObservations;
            this.totalCounts = totalCounts;
            this.countsByMode = countsByMode;
            this.effectiveRatio = effectiveRatio;
            this.neutralRatio = neutralRatio;
            this.negativeRatio = negativeRatio;
            this.decision = decision;
            this.observations = observations;
        }
    }

    public static final class DecisionPolicy {
        public final double effectiveMinPercent;
        public final double negativeMaxPercent;
        public final double enableIfEffectiveRatioAtLeast;
        public final double disableIfNegativeRatioAtLeast;

        DecisionPolicy(
                double effectiveMinPercent,
                double negativeMaxPercent,
                double enableIfEffectiveRatioAtLeast,
                double disableIfNegativeRatioAtLeast
        ) {
            this.effectiveMinPercent = effectiveMinPercent;
            this.negativeMaxPercent = negativeMaxPercent;
            this.enableIfEffectiveRatioAtLeast = enableIfEffectiveRatioAtLeast;
            this.disableIfNegativeRatioAtLeast = disableIfNegativeRatioAtLeast;
        }

        public static DecisionPolicy defaults() {
            return new DecisionPolicy(5.0d, -5.0d, 0.60d, 0.30d);
        }

        DecisionPolicy withOverride(String rawArg) {
            String arg = rawArg.substring(2);
            int eq = arg.indexOf('=');
            if (eq <= 0 || eq >= arg.length() - 1) {
                throw new IllegalArgumentException("invalid override: " + rawArg);
            }
            String key = arg.substring(0, eq);
            double value;
            try {
                value = Double.parseDouble(arg.substring(eq + 1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("invalid numeric override: " + rawArg, e);
            }
            if ("effectiveMinPercent".equals(key)) {
                return new DecisionPolicy(value, negativeMaxPercent, enableIfEffectiveRatioAtLeast, disableIfNegativeRatioAtLeast);
            }
            if ("negativeMaxPercent".equals(key)) {
                return new DecisionPolicy(effectiveMinPercent, value, enableIfEffectiveRatioAtLeast, disableIfNegativeRatioAtLeast);
            }
            if ("enableIfEffectiveRatioAtLeast".equals(key)) {
                return new DecisionPolicy(effectiveMinPercent, negativeMaxPercent, value, disableIfNegativeRatioAtLeast);
            }
            if ("disableIfNegativeRatioAtLeast".equals(key)) {
                return new DecisionPolicy(effectiveMinPercent, negativeMaxPercent, enableIfEffectiveRatioAtLeast, value);
            }
            throw new IllegalArgumentException("unsupported override key: " + key);
        }
    }
}
