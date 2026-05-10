/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lance.spark.internal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for the exception-redaction contract used throughout the Lance/Spark driver and
 * executor open paths.
 *
 * <p>Uses a sentinel URI carrying materials that MUST NOT appear in any log, exception message, or
 * stack-trace rendering of the resulting {@link IllegalStateException}:
 *
 * <ul>
 *   <li>S3-style userinfo: {@code user:secret@}
 *   <li>Query-string signature: {@code ?sig=XYZ&se=2024}
 *   <li>Non-sentinel host/path: asserted to also be absent from the wrapped message (we redact the
 *       entire URI, not just credentials).
 * </ul>
 */
class LanceExceptionsRedactionTest {

  private static final String CREDENTIALED_URI =
      "https://user:secret@example.invalid/bucket/path?sig=XYZ&se=2024#frag";

  @Test
  @DisplayName("wrap drops RuntimeException cause and omits URI fragments from message")
  void wrap_runtimeException_dropsCauseAndRedactsUri() {
    RuntimeException upstream = new RuntimeException("failed to connect to " + CREDENTIALED_URI);

    RuntimeException wrapped = LanceExceptions.wrap("open Lance dataset", upstream);

    assertThat(wrapped).isInstanceOf(IllegalStateException.class);
    assertThat(wrapped.getCause())
        .as("cause must be dropped so Spark UI 'Full stacktrace' cannot walk it and print the URI")
        .isNull();
    assertThat(wrapped.getMessage())
        .as("redacted message must not leak any part of the credentialed URI")
        .doesNotContain("secret")
        .doesNotContain("user:")
        .doesNotContain("sig=XYZ")
        .doesNotContain("XYZ")
        .doesNotContain("example.invalid")
        .doesNotContain("bucket");
    assertThat(wrapped.getMessage())
        .contains("URI redacted")
        .contains("underlying=java.lang.RuntimeException")
        .contains("open Lance dataset");
  }

  @Test
  @DisplayName("wrap rethrows Error subclasses unchanged without wrapping")
  void wrap_error_rethrowsAsIs() {
    OutOfMemoryError oom = new OutOfMemoryError("heap exhausted while opening " + CREDENTIALED_URI);

    assertThatThrownBy(() -> LanceExceptions.wrap("open Lance dataset", oom))
        .as("Error subclasses must propagate unchanged so JVM fatal-error handling runs")
        .isSameAs(oom);
  }

  @Test
  @DisplayName("wrap preserves operation label for diagnosis")
  void wrap_preservesOpLabel() {
    RuntimeException ex =
        LanceExceptions.wrap(
            "resolve latest Lance dataset version",
            new IllegalArgumentException("no matching manifest"));
    assertThat(ex.getMessage()).contains("resolve latest Lance dataset version");
  }

  @Test
  @DisplayName("redactUri strips userinfo, query, and fragment — keeps scheme://host/path")
  void redactUri_stripsCredentialsAndSignature() {
    String redacted = LanceExceptions.redactUri(CREDENTIALED_URI);
    assertThat(redacted)
        .doesNotContain("secret")
        .doesNotContain("user:")
        .doesNotContain("sig=XYZ")
        .doesNotContain("XYZ")
        .doesNotContain("frag");
    // Host + path are retained because they are not secrets and useful for operator diagnosis.
    assertThat(redacted).startsWith("https://example.invalid/bucket/path");
  }

  @Test
  @DisplayName("redactUri handles null gracefully")
  void redactUri_nullInput() {
    assertThat(LanceExceptions.redactUri(null)).isEqualTo("<null uri>");
  }

  @Test
  @DisplayName("redactUri returns sentinel for unparseable input rather than echoing raw")
  void redactUri_unparseableInput() {
    // An input with raw space characters fails URI parsing — we must NOT fall back to echoing
    // the raw string because it may still embed credentials.
    String malformed = "https://user:secret@host with space/path?sig=XYZ";
    String redacted = LanceExceptions.redactUri(malformed);
    assertThat(redacted).doesNotContain("secret").doesNotContain("XYZ");
  }

  @Test
  @DisplayName("redactUri strips userinfo from s3:// authority")
  void redactUri_s3UserinfoAuthority() {
    // Standard URI form for embedded credentials uses `user:password@host`.
    String redacted = LanceExceptions.redactUri("s3://AKIAxxxxxxxxxxxxxxxx:mysecret@bucket/path");
    assertThat(redacted).doesNotContain("AKIAxxxxxxxxxxxxxxxx").doesNotContain("mysecret");
    assertThat(redacted).contains("bucket");
  }
}
