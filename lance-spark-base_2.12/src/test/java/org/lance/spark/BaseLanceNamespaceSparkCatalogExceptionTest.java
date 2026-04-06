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
package org.lance.spark;

import org.lance.namespace.LanceNamespace;
import org.lance.namespace.errors.ErrorCode;
import org.lance.namespace.errors.LanceNamespaceException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.PermissionDeniedException;
import org.lance.namespace.errors.ServiceUnavailableException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.CreateNamespaceRequest;
import org.lance.namespace.model.CreateNamespaceResponse;
import org.lance.namespace.model.DeregisterTableRequest;
import org.lance.namespace.model.DeregisterTableResponse;
import org.lance.namespace.model.DescribeNamespaceRequest;
import org.lance.namespace.model.DescribeNamespaceResponse;
import org.lance.namespace.model.DropTableRequest;
import org.lance.namespace.model.DropTableResponse;
import org.lance.namespace.model.NamespaceExistsRequest;
import org.lance.namespace.model.TableExistsRequest;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests that {@link BaseLanceNamespaceSparkCatalog} propagates non-"not found" {@link
 * LanceNamespaceException}s instead of silently swallowing them.
 *
 * <p>Before the fix, all catch blocks used {@code catch (Exception e)} which swallowed errors like
 * PERMISSION_DENIED, SERVICE_UNAVAILABLE, etc. These tests verify that only the expected "not
 * found" errors are handled, and all others propagate.
 */
public class BaseLanceNamespaceSparkCatalogExceptionTest {

  private TestCatalog catalog;
  private ThrowingLanceNamespace mockNamespace;

  @BeforeEach
  void setUp() throws Exception {
    catalog = new TestCatalog();
    mockNamespace = new ThrowingLanceNamespace();

    // Inject mock namespace and required fields via reflection
    setField(catalog, "namespace", mockNamespace);
    setField(catalog, "pathBasedOnly", false);
    setField(catalog, "singleLevelNs", false);
    setField(catalog, "parentPrefix", org.lance.spark.utils.Optional.empty());
    setField(catalog, "name", "test-catalog");
  }

  // ── namespaceExists ────────────────────────────────────────────────

  @Nested
  @DisplayName("namespaceExists()")
  class NamespaceExistsTests {

    @Test
    @DisplayName("returns false for NAMESPACE_NOT_FOUND")
    void returnsFalseForNotFound() {
      mockNamespace.namespaceExistsAction =
          req -> {
            throw new NamespaceNotFoundException("ns not found");
          };
      assertFalse(catalog.namespaceExists(new String[] {"testns"}));
    }

    @Test
    @DisplayName("propagates PERMISSION_DENIED instead of returning false")
    void propagatesPermissionDenied() {
      mockNamespace.namespaceExistsAction =
          req -> {
            throw new PermissionDeniedException("access denied");
          };
      assertThrows(
          LanceNamespaceException.class, () -> catalog.namespaceExists(new String[] {"testns"}));
    }

    @Test
    @DisplayName("propagates SERVICE_UNAVAILABLE instead of returning false")
    void propagatesServiceUnavailable() {
      mockNamespace.namespaceExistsAction =
          req -> {
            throw new ServiceUnavailableException("server down");
          };
      LanceNamespaceException ex =
          assertThrows(
              LanceNamespaceException.class,
              () -> catalog.namespaceExists(new String[] {"testns"}));
      assertEquals(ErrorCode.SERVICE_UNAVAILABLE, ex.getErrorCode());
    }
  }

  // ── loadNamespaceMetadata ──────────────────────────────────────────

  @Nested
  @DisplayName("loadNamespaceMetadata()")
  class LoadNamespaceMetadataTests {

    @Test
    @DisplayName("propagates PERMISSION_DENIED instead of wrapping as NoSuchNamespaceException")
    void propagatesPermissionDenied() {
      mockNamespace.describeNamespaceAction =
          req -> {
            throw new PermissionDeniedException("access denied");
          };
      // Before fix: throws NoSuchNamespaceException (wrong)
      // After fix: throws LanceNamespaceException with PERMISSION_DENIED
      assertThrows(
          LanceNamespaceException.class,
          () -> catalog.loadNamespaceMetadata(new String[] {"testns"}));
    }
  }

  // ── createNamespace ────────────────────────────────────────────────

  @Nested
  @DisplayName("createNamespace()")
  class CreateNamespaceTests {

    @Test
    @DisplayName("throws NamespaceAlreadyExistsException via typed exception, not string matching")
    void handlesAlreadyExistsViaTypedException() {
      // Throw the lance-namespace typed exception with a NON-English message
      // that does NOT contain "already exists" — the old string-matching code would miss this
      mockNamespace.createNamespaceAction =
          req -> {
            throw new org.lance.namespace.errors.NamespaceAlreadyExistsException(
                "命名空间已存在"); // Chinese: "namespace already exists"
          };
      // Before fix: throws RuntimeException("Failed to create namespace")
      //   because getMessage().contains("already exists") fails on non-English messages
      // After fix: catches the typed exception directly
      assertThrows(
          org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException.class,
          () -> catalog.createNamespace(new String[] {"testns"}, Map.of()));
    }
  }

  // ── tableExists ────────────────────────────────────────────────────

  @Nested
  @DisplayName("tableExists()")
  class TableExistsTests {

    @Test
    @DisplayName("returns false for TABLE_NOT_FOUND")
    void returnsFalseForNotFound() {
      mockNamespace.tableExistsAction =
          req -> {
            throw new TableNotFoundException("table not found");
          };
      assertFalse(catalog.tableExists(Identifier.of(new String[] {"ns"}, "tbl")));
    }

    @Test
    @DisplayName("propagates PERMISSION_DENIED instead of returning false")
    void propagatesPermissionDenied() {
      mockNamespace.tableExistsAction =
          req -> {
            throw new PermissionDeniedException("access denied");
          };
      assertThrows(
          LanceNamespaceException.class,
          () -> catalog.tableExists(Identifier.of(new String[] {"ns"}, "tbl")));
    }
  }

  // ── dropTable ──────────────────────────────────────────────────────

  @Nested
  @DisplayName("dropTable()")
  class DropTableTests {

    @Test
    @DisplayName("returns false for TABLE_NOT_FOUND")
    void returnsFalseForNotFound() {
      mockNamespace.deregisterTableAction =
          req -> {
            throw new TableNotFoundException("table not found");
          };
      assertFalse(catalog.dropTable(Identifier.of(new String[] {"ns"}, "tbl")));
    }

    @Test
    @DisplayName("propagates SERVICE_UNAVAILABLE instead of returning false")
    void propagatesServiceUnavailable() {
      mockNamespace.deregisterTableAction =
          req -> {
            throw new ServiceUnavailableException("server down");
          };
      assertThrows(
          LanceNamespaceException.class,
          () -> catalog.dropTable(Identifier.of(new String[] {"ns"}, "tbl")));
    }
  }

  // ── purgeTable ─────────────────────────────────────────────────────

  @Nested
  @DisplayName("purgeTable()")
  class PurgeTableTests {

    @Test
    @DisplayName("propagates PERMISSION_DENIED instead of returning false")
    void propagatesPermissionDenied() {
      mockNamespace.dropTableAction =
          req -> {
            throw new PermissionDeniedException("access denied");
          };
      assertThrows(
          LanceNamespaceException.class,
          () -> catalog.purgeTable(Identifier.of(new String[] {"ns"}, "tbl")));
    }
  }

  // ── Test infrastructure ────────────────────────────────────────────

  /** Minimal concrete subclass of BaseLanceNamespaceSparkCatalog for testing. */
  private static class TestCatalog extends BaseLanceNamespaceSparkCatalog {
    @Override
    public LanceDataset createDataset(
        LanceSparkReadOptions readOptions,
        org.apache.spark.sql.types.StructType sparkSchema,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        boolean managedVersioning,
        String fileFormatVersion) {
      throw new UnsupportedOperationException("not needed for exception tests");
    }

    @Override
    public LanceDataset createStagedDataset(
        LanceSparkReadOptions readOptions,
        org.apache.spark.sql.types.StructType sparkSchema,
        Map<String, String> initialStorageOptions,
        String namespaceImpl,
        Map<String, String> namespaceProperties,
        boolean managedVersioning,
        org.lance.spark.write.StagedCommit stagedCommit,
        String fileFormatVersion) {
      throw new UnsupportedOperationException("not needed for exception tests");
    }
  }

  /**
   * A LanceNamespace implementation whose behavior is controlled via functional fields. Each method
   * delegates to a configurable action, defaulting to UnsupportedOperationException.
   */
  private static class ThrowingLanceNamespace implements LanceNamespace {

    @FunctionalInterface
    interface NamespaceExistsAction {
      void execute(NamespaceExistsRequest request);
    }

    @FunctionalInterface
    interface DescribeNamespaceAction {
      DescribeNamespaceResponse execute(DescribeNamespaceRequest request);
    }

    @FunctionalInterface
    interface CreateNamespaceAction {
      CreateNamespaceResponse execute(CreateNamespaceRequest request);
    }

    @FunctionalInterface
    interface TableExistsAction {
      void execute(TableExistsRequest request);
    }

    @FunctionalInterface
    interface DeregisterTableAction {
      DeregisterTableResponse execute(DeregisterTableRequest request);
    }

    @FunctionalInterface
    interface DropTableAction {
      DropTableResponse execute(DropTableRequest request);
    }

    NamespaceExistsAction namespaceExistsAction;
    DescribeNamespaceAction describeNamespaceAction;
    CreateNamespaceAction createNamespaceAction;
    TableExistsAction tableExistsAction;
    DeregisterTableAction deregisterTableAction;
    DropTableAction dropTableAction;

    @Override
    public void initialize(Map<String, String> configProperties, BufferAllocator allocator) {
      // no-op
    }

    @Override
    public String namespaceId() {
      return "test-namespace";
    }

    @Override
    public void namespaceExists(NamespaceExistsRequest request) {
      if (namespaceExistsAction != null) {
        namespaceExistsAction.execute(request);
      }
    }

    @Override
    public DescribeNamespaceResponse describeNamespace(DescribeNamespaceRequest request) {
      if (describeNamespaceAction != null) {
        return describeNamespaceAction.execute(request);
      }
      return new DescribeNamespaceResponse();
    }

    @Override
    public CreateNamespaceResponse createNamespace(CreateNamespaceRequest request) {
      if (createNamespaceAction != null) {
        return createNamespaceAction.execute(request);
      }
      return new CreateNamespaceResponse();
    }

    @Override
    public void tableExists(TableExistsRequest request) {
      if (tableExistsAction != null) {
        tableExistsAction.execute(request);
      }
    }

    @Override
    public DeregisterTableResponse deregisterTable(DeregisterTableRequest request) {
      if (deregisterTableAction != null) {
        return deregisterTableAction.execute(request);
      }
      return new DeregisterTableResponse();
    }

    @Override
    public DropTableResponse dropTable(DropTableRequest request) {
      if (dropTableAction != null) {
        return dropTableAction.execute(request);
      }
      return new DropTableResponse();
    }
  }

  private static void setField(Object target, String fieldName, Object value) throws Exception {
    Class<?> clazz = target.getClass();
    while (clazz != null) {
      try {
        Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
        return;
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      }
    }
    throw new NoSuchFieldException(fieldName);
  }
}
