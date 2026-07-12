package com.dr.producer;

/**
 * Minimal manual CLI parser -- three flags don't justify pulling in picocli
 * or similar as a dependency.
 */
public class Args {
    int count = -1;               // no sensible default -- must be supplied
    String bootstrapServers = "localhost:9092";
    String topic = "commit-log";

    public static Args parse(String[] argv) {
        Args a = new Args();
        for (int i = 0; i < argv.length; i++) {
            String flag = argv[i];
            switch (flag) {
                case "--count":
                    a.count = parseCount(requireValue(argv, i, flag));
                    i++;
                    break;
                case "--bootstrap-servers":
                    a.bootstrapServers = requireValue(argv, i, flag);
                    i++;
                    break;
                case "--topic":
                    a.topic = requireValue(argv, i, flag);
                    i++;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown argument: " + flag);
            }
        }
        if (a.count <= 0) {
            throw new IllegalArgumentException("--count is required and must be a positive integer");
        }
        return a;
    }

    /** Returns the value following flagIndex, or throws a clear error naming the flag -- not an ArrayIndexOutOfBoundsException. */
    private static String requireValue(String[] argv, int flagIndex, String flagName) {
        if (flagIndex + 1 >= argv.length) {
            throw new IllegalArgumentException(flagName + " requires a value but none was provided");
        }
        return argv[flagIndex + 1];
    }

    /** Parses --count's value, or throws a message naming the flag -- not a raw NumberFormatException. */
    private static int parseCount(String raw) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--count must be a valid integer, got: '" + raw + "'");
        }
    }
}