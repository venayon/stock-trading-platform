package com.example.finance.feed;

import java.io.*;
import java.nio.file.*;
import java.security.*;
import java.util.HexFormat;

public final class FeedFiles {
    private FeedFiles() {
    }

    static Path identityPath(Path source) {
        return source.resolveSibling(source.getFileName() + ".identity");
    }

    /**
     * Keep original identity across scans, even if configuration changes after a technical failure.
     */
    public static Identity identify(Path source, String currentPolicy) throws IOException {
        String actual = hash(source);
        Path manifest = identityPath(source);
        if (Files.exists(manifest, LinkOption.NOFOLLOW_LINKS)) {
            if (!Files.isRegularFile(manifest, LinkOption.NOFOLLOW_LINKS) || Files.size(manifest) > 128)
                throw new IOException("Invalid feed identity manifest");
            String[] fields = Files.readString(manifest).strip().split("\\n");
            if (fields.length != 2
                || !fields[0].matches("[0-9a-f]{64}")
                || !fields[1].matches("[A-Za-z0-9_-]{1,32}"))
                throw new IOException("Invalid feed identity manifest");
            if (!actual.equals(fields[0]))
                throw new IOException("Processing file changed; restore original before restart");
            return new Identity(fields[0], fields[1]);
        }
        Files.writeString(
            manifest, actual + "\n" + currentPolicy + "\n", StandardOpenOption.CREATE_NEW);
        return new Identity(actual, currentPolicy);
    }

    public static String hash(Path path) throws IOException {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream input = Files.newInputStream(path)) {
                byte[] buffer = new byte[65536];
                int n;
                while ((n = input.read(buffer)) != -1) digest.update(buffer, 0, n);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public record Identity(String hash, String policy) {
    }
}
