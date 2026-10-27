import type { ReactNode } from 'react';
import { Navigate, Route, Routes } from 'react-router-dom';

import { AppShell } from './components/AppShell';
import { RequirePermission, SessionGate } from './features/auth/AccessGate';
import { SignInPage } from './features/auth/SignInPage';
import { SsoOutcomeToast } from './features/auth/SsoOutcome';
import { DatasetPage } from './features/datasets/DatasetPage';
import { DatasetsPage } from './features/datasets/DatasetsPage';
import { DetailPage } from './features/detail/DetailPage';
import { MaterializationsPage } from './features/materializations/MaterializationsPage';
import { OverviewPage } from './features/overview/OverviewPage';
import { SearchPage } from './features/search/SearchPage';
import { SettingsPage } from './features/settings/SettingsPage';
import { DomainScope, domainPath, useDomain } from './lib/domains';

/**
 * The console's pages (handbook 20): every domain at `/`, `/materializations`, `/datasets` and
 * `/search`; the console's own settings at `/settings`; everything about one domain under
 * `/d/<domain>/`.
 */
export function App(): ReactNode {
  return (
    <SessionGate>
      <SsoOutcomeToast />
      <Routes>
        <Route path="/sign-in" element={<SignInPage />} />
        <Route path="/sign-in/:domain" element={<SignInPage />} />
        <Route path="*" element={<ShellRoutes />} />
      </Routes>
    </SessionGate>
  );
}

function ShellRoutes(): ReactNode {
  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<Overview />} />
        <Route path="/materializations" element={<Materializations />} />
        <Route path="/datasets" element={<Datasets />} />
        <Route path="/search" element={<SearchPage />} />
        <Route path="/settings" element={<SettingsPage />} />
        <Route path="/settings/:section" element={<SettingsPage />} />
        <Route path="/settings/:section/:itemId" element={<SettingsPage />} />
        <Route
          path="/d/:domain/*"
          element={
            <DomainScope>
              <DomainRoutes />
            </DomainScope>
          }
        />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </AppShell>
  );
}

/** One domain's pages, under `/d/<domain>/`. */
function DomainRoutes(): ReactNode {
  return (
    <Routes>
      <Route index element={<Overview />} />
      <Route path="materializations" element={<Materializations />} />
      <Route
        path="materializations/:materializationId"
        element={
          <RequirePermission permission="materializations.view_details" what="this materialization">
            <DetailPage />
          </RequirePermission>
        }
      />
      <Route
        path="materializations/:materializationId/:tab"
        element={
          <RequirePermission permission="materializations.view_details" what="this materialization">
            <DetailPage />
          </RequirePermission>
        }
      />
      <Route path="datasets" element={<Datasets />} />
      <Route
        path="datasets/:name"
        element={
          <RequirePermission permission="datasets.view" what="datasets">
            <DatasetPage />
          </RequirePermission>
        }
      />
      <Route
        path="datasets/:name/:tab"
        element={
          <RequirePermission permission="datasets.view" what="datasets">
            <DatasetPage />
          </RequirePermission>
        }
      />
      <Route path="settings" element={<SettingsPage />} />
      <Route path="settings/:section" element={<SettingsPage />} />
      <Route path="settings/:section/:itemId" element={<SettingsPage />} />
      <Route path="*" element={<DomainHome />} />
    </Routes>
  );
}

function Overview(): ReactNode {
  return (
    <RequirePermission permission="materializations.view" what="the overview">
      <OverviewPage />
    </RequirePermission>
  );
}

function Materializations(): ReactNode {
  return (
    <RequirePermission permission="materializations.view" what="materializations">
      <MaterializationsPage />
    </RequirePermission>
  );
}

function Datasets(): ReactNode {
  return (
    <RequirePermission permission="datasets.view" what="datasets">
      <DatasetsPage />
    </RequirePermission>
  );
}

/** An address inside a domain that names no page: the domain's overview. */
function DomainHome(): ReactNode {
  const { id } = useDomain();
  return <Navigate to={domainPath(id)} replace />;
}
