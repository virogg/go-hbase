// Copyright 2026 The go-hbase Authors
// SPDX-License-Identifier: Apache-2.0

package com.virogg.hbasecop.bridge.supervisor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class ManifestBinaryDescriptorTest {

  // 64-hex SHA-256 - content here is opaque to the descriptor (it only checks shape/length).
  private static final String SHA256_OK =
      "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

  @Test
  void parsesAllFourHbaseCopFields(@TempDir Path dir) throws IOException {
    Path jar = dir.resolve("with-fields.jar");
    Map<String, String> mf = new LinkedHashMap<>();
    mf.put("HbaseCop-Observer-Class", "com.example.Audit");
    mf.put("HbaseCop-Coproc-Id", "audit-observer");
    mf.put("HbaseCop-Go-Bin-Name", "bin/linux-amd64/audit");
    mf.put("HbaseCop-Go-Bin-SHA256", SHA256_OK);
    writeJar(jar, mf);

    ManifestBinaryDescriptor d = ManifestBinaryDescriptor.fromJar(jar);

    assertNotNull(d);
    assertEquals("com.example.Audit", d.observerClass());
    assertEquals("audit-observer", d.coprocId());
    assertEquals("bin/linux-amd64/audit", d.binaryResourcePath());
    assertEquals(SHA256_OK, d.binarySha256());
  }

  @Test
  void normalisesSha256ToLowerCase(@TempDir Path dir) throws IOException {
    Path jar = dir.resolve("upper-sha.jar");
    Map<String, String> mf = new LinkedHashMap<>();
    mf.put("HbaseCop-Go-Bin-Name", "bin/linux-amd64/x");
    mf.put("HbaseCop-Go-Bin-SHA256", SHA256_OK.toUpperCase());
    writeJar(jar, mf);

    ManifestBinaryDescriptor d = ManifestBinaryDescriptor.fromJar(jar);
    assertEquals(SHA256_OK, d.binarySha256()); // lower-case canonical
  }

  @Test
  void absentWhenNoHbaseCopFields(@TempDir Path dir) throws IOException {
    Path jar = dir.resolve("vanilla.jar");
    Map<String, String> mf = new LinkedHashMap<>();
    mf.put("Built-By", "tester");
    writeJar(jar, mf);

    assertNull(ManifestBinaryDescriptor.fromJar(jar));
  }

  @Test
  void nameWithoutSha256Rejected(@TempDir Path dir) throws IOException {
    Path jar = dir.resolve("orphan-name.jar");
    Map<String, String> mf = new LinkedHashMap<>();
    mf.put("HbaseCop-Go-Bin-Name", "bin/linux-amd64/x");
    writeJar(jar, mf);

    IOException ex = assertThrows(IOException.class, () -> ManifestBinaryDescriptor.fromJar(jar));
    assertTrue(ex.getMessage().contains("HbaseCop-Go-Bin-SHA256"));
  }

  @Test
  void sha256WithoutNameRejected(@TempDir Path dir) throws IOException {
    Path jar = dir.resolve("orphan-sha.jar");
    Map<String, String> mf = new LinkedHashMap<>();
    mf.put("HbaseCop-Go-Bin-SHA256", SHA256_OK);
    writeJar(jar, mf);

    IOException ex = assertThrows(IOException.class, () -> ManifestBinaryDescriptor.fromJar(jar));
    assertTrue(ex.getMessage().contains("HbaseCop-Go-Bin-Name"));
  }

  @Test
  void malformedSha256Rejected(@TempDir Path dir) throws IOException {
    Path jar = dir.resolve("bad-sha.jar");
    Map<String, String> mf = new LinkedHashMap<>();
    mf.put("HbaseCop-Go-Bin-Name", "bin/linux-amd64/x");
    mf.put("HbaseCop-Go-Bin-SHA256", "zz-not-hex");
    writeJar(jar, mf);

    assertThrows(IOException.class, () -> ManifestBinaryDescriptor.fromJar(jar));
  }

  @Test
  void shortSha256Rejected(@TempDir Path dir) throws IOException {
    Path jar = dir.resolve("short-sha.jar");
    Map<String, String> mf = new LinkedHashMap<>();
    mf.put("HbaseCop-Go-Bin-Name", "bin/linux-amd64/x");
    mf.put("HbaseCop-Go-Bin-SHA256", "deadbeef"); // 8 hex chars, not 64
    writeJar(jar, mf);

    assertThrows(IOException.class, () -> ManifestBinaryDescriptor.fromJar(jar));
  }

  @Test
  void missingJarFileSurfacesIoException(@TempDir Path dir) {
    Path jar = dir.resolve("nope.jar");
    assertThrows(IOException.class, () -> ManifestBinaryDescriptor.fromJar(jar));
  }

  @Test
  void optionalIdAndClassAllowed(@TempDir Path dir) throws IOException {
    // CLI omitted observer-class/coproc-id (e.g. legacy jar bundled by another tool).
    // Binary descriptor still loads as long as Name+SHA256 are both present.
    Path jar = dir.resolve("min.jar");
    Map<String, String> mf = new LinkedHashMap<>();
    mf.put("HbaseCop-Go-Bin-Name", "bin/linux-amd64/x");
    mf.put("HbaseCop-Go-Bin-SHA256", SHA256_OK);
    writeJar(jar, mf);

    ManifestBinaryDescriptor d = ManifestBinaryDescriptor.fromJar(jar);
    assertNotNull(d);
    assertNull(d.observerClass());
    assertNull(d.coprocId());
    assertEquals("bin/linux-amd64/x", d.binaryResourcePath());
  }

  // --- helpers ---

  private static void writeJar(Path path, Map<String, String> hbaseCopAttrs) throws IOException {
    Manifest m = new Manifest();
    Attributes a = m.getMainAttributes();
    a.putValue("Manifest-Version", "1.0");
    for (Map.Entry<String, String> e : hbaseCopAttrs.entrySet()) {
      a.putValue(e.getKey(), e.getValue());
    }
    try (OutputStream os = Files.newOutputStream(path);
        JarOutputStream jos = new JarOutputStream(os, m)) {
      // empty payload - manifest only is enough
      jos.putNextEntry(new JarEntry("dummy.txt"));
      jos.write("noop".getBytes(StandardCharsets.UTF_8));
      jos.closeEntry();
    }
  }
}
