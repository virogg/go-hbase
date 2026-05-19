// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.supervisor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.regex.Pattern;

/**
 * Parsed view of the {@code HbaseCop-*} entries emitted by {@code hbasecop-build} into a coproc-jar
 * manifest. Used by the supervisor to (a) locate the embedded Go ELF inside the jar and (b) verify
 * its SHA-256 digest at extract time — guarding against jar corruption or wrong-arch binaries.
 *
 * <p>Two manifest attributes are paired and mutually required: {@code HbaseCop-Go-Bin-Name}
 * (resource path inside the jar, e.g. {@code bin/linux-amd64/audit}) and {@code
 * HbaseCop-Go-Bin-SHA256} (lower-case 64-hex digest of the ELF bytes). The remaining attributes
 * {@code HbaseCop-Observer-Class} and {@code HbaseCop-Coproc-Id} are optional metadata for
 * registration tooling.
 *
 * <p>A jar with none of these attributes returns {@code null} from {@link #fromJar(Path)} — that is
 * not an error, only an indication that the jar was not produced by {@code hbasecop-build}.
 */
public final class ManifestBinaryDescriptor {

  private static final String ATTR_OBSERVER_CLASS = "HbaseCop-Observer-Class";
  private static final String ATTR_COPROC_ID = "HbaseCop-Coproc-Id";
  private static final String ATTR_BIN_NAME = "HbaseCop-Go-Bin-Name";
  private static final String ATTR_BIN_SHA256 = "HbaseCop-Go-Bin-SHA256";

  private static final Pattern HEX64 = Pattern.compile("[0-9a-f]{64}");

  private final String observerClass;
  private final String coprocId;
  private final String binaryResourcePath;
  private final String binarySha256;

  private ManifestBinaryDescriptor(
      String observerClass, String coprocId, String binaryResourcePath, String binarySha256) {
    this.observerClass = observerClass;
    this.coprocId = coprocId;
    this.binaryResourcePath = binaryResourcePath;
    this.binarySha256 = binarySha256;
  }

  /** Fully-qualified Java class implementing the observer; may be {@code null}. */
  public String observerClass() {
    return observerClass;
  }

  /** Operator-chosen identifier for this coprocessor (e.g. {@code audit-observer}); nullable. */
  public String coprocId() {
    return coprocId;
  }

  /** Jar-internal resource path of the embedded Go ELF; never {@code null}. */
  public String binaryResourcePath() {
    return binaryResourcePath;
  }

  /** Lower-case 64-hex SHA-256 digest of the ELF bytes; never {@code null}. */
  public String binarySha256() {
    return binarySha256;
  }

  /**
   * Read and validate the {@code HbaseCop-*} manifest entries from {@code jarPath}.
   *
   * @return descriptor, or {@code null} when the jar has no {@code HbaseCop-*} entries
   * @throws IOException if the jar is missing, unreadable, or its {@code HbaseCop-*} entries are
   *     internally inconsistent (e.g. name without sha256, malformed digest)
   */
  public static ManifestBinaryDescriptor fromJar(Path jarPath) throws IOException {
    try (InputStream is = Files.newInputStream(jarPath);
        JarInputStream jis = new JarInputStream(is)) {
      Manifest mf = jis.getManifest();
      if (mf == null) {
        return null;
      }
      return fromAttributes(mf.getMainAttributes());
    }
  }

  /**
   * Build a descriptor from an already-parsed {@link Attributes} block (e.g. the main attributes of
   * a {@link Manifest} loaded from a classpath resource).
   *
   * @return descriptor, or {@code null} when the attributes carry no {@code HbaseCop-*} keys
   * @throws IOException on internal inconsistency (see {@link #fromJar(Path)})
   */
  public static ManifestBinaryDescriptor fromAttributes(Attributes a) throws IOException {
    String observerClass = trimToNull(a.getValue(ATTR_OBSERVER_CLASS));
    String coprocId = trimToNull(a.getValue(ATTR_COPROC_ID));
    String binName = trimToNull(a.getValue(ATTR_BIN_NAME));
    String binSha = trimToNull(a.getValue(ATTR_BIN_SHA256));

    boolean anyPresent =
        observerClass != null || coprocId != null || binName != null || binSha != null;
    if (!anyPresent) {
      return null;
    }
    if (binName == null) {
      throw new IOException(
          "coproc-jar manifest: " + ATTR_BIN_SHA256 + " present without " + ATTR_BIN_NAME);
    }
    if (binSha == null) {
      throw new IOException(
          "coproc-jar manifest: " + ATTR_BIN_NAME + " present without " + ATTR_BIN_SHA256);
    }
    String shaLower = binSha.toLowerCase(Locale.ROOT);
    if (!HEX64.matcher(shaLower).matches()) {
      throw new IOException(
          "coproc-jar manifest: "
              + ATTR_BIN_SHA256
              + " must be 64 lower-case hex chars (got "
              + binSha.length()
              + " chars: "
              + binSha
              + ")");
    }
    return new ManifestBinaryDescriptor(observerClass, coprocId, binName, shaLower);
  }

  private static String trimToNull(String s) {
    if (s == null) {
      return null;
    }
    String t = s.trim();
    return t.isEmpty() ? null : t;
  }
}
