package org.apache.hadoop.fs.shell;

import picocli.CommandLine;

public class SuffixArgConverter implements CommandLine.ITypeConverter<Long> {
    private enum SUFFIX {
        KILO(1024, "k", "kb"),
        MEGA(1024 * 1024, "m", "mb"),
        GIGA(1024 * 1024 * 1024, "g", "gb"),
        ;

        private final long coefficient;
        private final String[] variants;

        SUFFIX(long coefficient, String... variants) {
            this.coefficient = coefficient;
            this.variants = variants;
        }

        static long convert(String value) {
            for (SUFFIX suffix : SUFFIX.values()) {
                for (String variant : suffix.variants) {
                    if (value.endsWith(variant)) {
                        long result = Long.parseLong(value.substring(0, value.length() - variant.length()));
                        return result * suffix.coefficient;
                    }
                }
            }
            return Long.parseLong(value);
        }
    }
    /**
     * Converts the specified command line argument value to some domain object.
     *
     * @param value the command line argument String value
     * @return the resulting domain object
     * @throws Exception an exception detailing what went wrong during the conversion
     */
    @Override
    public Long convert(String value) throws Exception {
        return SUFFIX.convert(value.toLowerCase());
    }
}
