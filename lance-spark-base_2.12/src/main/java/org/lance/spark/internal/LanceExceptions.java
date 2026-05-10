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

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Helpers for translating exceptions thrown by Lance native / JNI / object-store layers into
 * exceptions safe to surface through Spark (UI "Full stacktrace", driver logs, executor logs).
 *
 * <p>The problem: upstream Lance Rust / object_store errors embed the raw dataset URI into their
 * message. That URI often carries credentials — S3 userinfo ({@code s3://AKIA...:SECRET@bucket}),
 * Azure SAS query params ({@code ?sig=...&se=...}), pre-signed GCS/S3 signatures — which must never
 * reach a log line, Spark UI, or an end-user error response.
 *
 * <p>Preserving the original {@link Throwable} as a cause is not enough redaction: {@code
 * Throwable.printStackTrace}, Spark UI's "Full stacktrace" panel, and SLF4J's trailing-Throwable
 * behavior all walk the cause chain and emit each cause's {@code getMessage()}. Even with a
 * redacted wrapper message, the chain will still leak the underlying URI. The safe pattern is to
 * drop the cause entirely for exception classes that may embed the URI (any {@link
 * RuntimeException} from the upstream open / checkout / JNI path) and surface only the exception
 * class name for diagnosis.
 *
 * <p>{@link Error} subclasses (OOM, LinkageError, StackOverflowError) are re-thrown as-is — they
 * carry no URI in their message by convention and must propagate unchanged so the JVM's usual
 * fatal-error handling takes over.
 */
public final class LanceExceptions {

  private LanceExceptions() {}

  /**
   * Translates a driver-side Lance exception into a redacting {@link IllegalStateException}, or
   * re-throws the exception unchanged if it is an {@link Error}.
   *
   * <p>Usage:
   *
   * <pre>{@code
   * try {
   *   resolved = Dataset.latestVersionId(uri, merged);
   * } catch (Throwable ex) {
   *   throw LanceExceptions.wrap("resolve latest Lance dataset version", ex);
   * }
   * }</pre>
   *
   * @param op short action label embedded into the resulting message, e.g. "open Lance dataset".
   *     Must not contain any URI / credential material — this string is logged verbatim.
   * @param ex the upstream throwable whose message may embed the raw URI. If it is an {@link
   *     Error}, this method throws it immediately and does not return.
   * @return an {@link IllegalStateException} with no {@code cause} set, carrying only a redacted
   *     message plus the class name of {@code ex} for diagnosis.
   * @throws Error if {@code ex} is an {@link Error} subclass (rethrown as-is).
   */
  public static RuntimeException wrap(String op, Throwable ex) {
    if (ex instanceof Error) {
      throw (Error) ex;
    }
    return new IllegalStateException(
        "Failed to " + op + " (URI redacted, underlying=" + ex.getClass().getName() + ")");
  }

  /**
   * Returns a loggable form of {@code rawUri} safe for emitting into user-facing logs / UIs.
   *
   * <p>Strips userinfo ({@code user:password@}), query string (may contain SAS tokens / signed-URL
   * parameters / access tokens), and fragment. Retains only {@code scheme://host[:port]/path}.
   *
   * <p>If parsing fails (malformed URI), returns the literal string {@code "<unparseable uri>"}
   * rather than falling back to the raw input — a malformed URI that cannot be normalized may still
   * contain embedded credentials.
   *
   * @param rawUri the full dataset URI (may be null, in which case this returns {@code "<null
   *     uri>"}).
   * @return a redacted representation of {@code rawUri} suitable for logging.
   */
  public static String redactUri(String rawUri) {
    if (rawUri == null) {
      return "<null uri>";
    }
    try {
      URI parsed = new URI(rawUri);
      StringBuilder sb = new StringBuilder();
      if (parsed.getScheme() != null) {
        sb.append(parsed.getScheme()).append("://");
      }
      if (parsed.getHost() != null) {
        sb.append(parsed.getHost());
        if (parsed.getPort() >= 0) {
          sb.append(':').append(parsed.getPort());
        }
      } else if (parsed.getAuthority() != null) {
        // Authority parsed but host didn't resolve (e.g. "bucket" in s3://bucket/path forms).
        // Strip any embedded userinfo defensively.
        String authority = parsed.getAuthority();
        int at = authority.lastIndexOf('@');
        sb.append(at >= 0 ? authority.substring(at + 1) : authority);
      }
      if (parsed.getRawPath() != null) {
        sb.append(parsed.getRawPath());
      }
      return sb.length() == 0 ? "<unparseable uri>" : sb.toString();
    } catch (URISyntaxException ignored) {
      return "<unparseable uri>";
    }
  }
}
