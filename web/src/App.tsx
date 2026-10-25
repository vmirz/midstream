import type { ReactNode } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';

import { AppShell } from './components/AppShell';
import { RequirePermission, SessionGate } from './features/auth/AccessGate';
import { SignInPage } from './features/auth/SignInPage';
import { DatasetPage } from './features/datasets/DatasetPage';
import { DatasetsPage } from './features/datasets/DatasetsPage';
import { DetailPage } from './features/detail/DetailPage';
import { MaterializationsPage } from './features/materializations/MaterializationsPage';
import { OverviewPage } from './features/overview/OverviewPage';
import { SettingsPage } from './features/settings/SettingsPage';

export function App(): ReactNode {
  return (
    <SessionGate>
      <Routes>
        <Route path="/sign-in" element={<SignInPage />} />
        <Route path="*" element={<ShellRoutes />} />
      </Routes>
    </SessionGate>
  );
}

function ShellRoutes(): ReactNode {
  return (
    <AppShell>
      <Routes>
        <Route
          path="/"
          element={
            <RequirePermission permission="materializations.view" what="the overview">
              <OverviewPage />
            </RequirePermission>
          }
        />
        <Route
          path="/materializations"
          element={
            <RequirePermission permission="materializations.view" what="materializations">
              <MaterializationsPage />
            </RequirePermission>
          }
        />
        <Route
          path="/materializations/:materializationId"
          element={
            <RequirePermission permission="materializations.view_details" what="this materialization">
              <DetailPage />
            </RequirePermission>
          }
        />
        <Route
          path="/materializations/:materializationId/:tab"
          element={
            <RequirePermission permission="materializations.view_details" what="this materialization">
              <DetailPage />
            </RequirePermission>
          }
        />
        <Route
          path="/datasets"
          element={
            <RequirePermission permission="datasets.view" what="datasets">
              <DatasetsPage />
            </RequirePermission>
          }
        />
        <Route
          path="/datasets/:name"
          element={
            <RequirePermission permission="datasets.view" what="datasets">
              <DatasetPage />
            </RequirePermission>
          }
        />
        <Route
          path="/datasets/:name/:tab"
          element={
            <RequirePermission permission="datasets.view" what="datasets">
              <DatasetPage />
            </RequirePermission>
          }
        />
        <Route path="/settings" element={<SettingsPage />} />
        <Route path="/settings/:section" element={<SettingsPage />} />
        <Route path="/settings/:section/:itemId" element={<SettingsPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </AppShell>
  );
}
