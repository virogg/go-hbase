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

  public String observerClass() {
    return observerClass;
  }

  public String coprocId() {
    return coprocId;
  }

  public String binaryResourcePath() {
    return binaryResourcePath;
  }

  public String binarySha256() {
    return binarySha256;
  }

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
