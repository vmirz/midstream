import type { ReactNode } from 'react';

import type { SettingsSection } from '../settings/sections';
import { AccountOverview } from './AccountOverview';
import { AuditSection } from './AuditSection';
import { GroupDetail } from './GroupDetail';
import { GroupsSection } from './GroupsSection';
import { RoleEditor } from './RoleEditor';
import { RolesSection } from './RolesSection';
import { UserDetail } from './UserDetail';
import { UsersSection } from './UsersSection';

/**
 * The Settings sections that are pages of their own rather than lists of settings: your account, on
 * the console's page (`itemId` the domain shown); a domain's users, groups, roles and audit log, on
 * that domain's.
 */
export function AccessSectionContent({ section, itemId }: { section: SettingsSection; itemId: string | null }): ReactNode {
  switch (section.kind) {
    case 'account':
      return <AccountOverview domainId={itemId} />;
    case 'users':
      return itemId ? <UserDetail userId={itemId} /> : <UsersSection />;
    case 'groups':
      return itemId ? <GroupDetail groupId={itemId} /> : <GroupsSection />;
    case 'roles':
      return itemId ? <RoleEditor roleId={itemId} /> : <RolesSection />;
    case 'audit':
      return <AuditSection />;
    default:
      return null;
  }
}
