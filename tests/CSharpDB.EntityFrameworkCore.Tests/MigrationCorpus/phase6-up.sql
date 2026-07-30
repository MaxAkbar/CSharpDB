CREATE TABLE IF NOT EXISTS __EFMigrationsHistory (
    MigrationId TEXT NOT NULL PRIMARY KEY,
    ProductVersion TEXT NOT NULL
);
START TRANSACTION;
CREATE TABLE "Organizations" (
    "TenantId" INTEGER NOT NULL,
    "OrganizationId" INTEGER NOT NULL,
    "Name" TEXT NOT NULL DEFAULT 'unnamed',
    CONSTRAINT "CK_Organizations_Name" CHECK (Name <> ''),
    CONSTRAINT "PK_Organizations" PRIMARY KEY ("TenantId", "OrganizationId")
);

CREATE TABLE "WorkItems" (
    "TenantId" INTEGER NOT NULL,
    "TaskId" INTEGER NOT NULL,
    "OrganizationId" INTEGER NOT NULL,
    "Title" TEXT NOT NULL DEFAULT 'untitled',
    "State" TEXT NOT NULL DEFAULT 'open',
    CONSTRAINT "CK_WorkItems_State" CHECK (State IN ('open', 'closed')),
    CONSTRAINT "FK_WorkItems_Organizations_TenantId_OrganizationId" FOREIGN KEY ("TenantId", "OrganizationId") REFERENCES "Organizations" ("TenantId", "OrganizationId") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "PK_WorkItems" PRIMARY KEY ("TenantId", "TaskId")
);

CREATE INDEX "IX_WorkItems_TenantId_OrganizationId" ON "WorkItems" ("TenantId", "OrganizationId");

CREATE TABLE "MemberProfiles" (
    "Id" INTEGER NOT NULL,
    "Code" TEXT NOT NULL DEFAULT 'legacy',
    "Rating" INTEGER NOT NULL DEFAULT 1,
    "State" TEXT NOT NULL DEFAULT 'active',
    CONSTRAINT "CK_Members_Rating" CHECK (Rating >= 0),
    CONSTRAINT "PK_Members" PRIMARY KEY ("Id")
);

CREATE INDEX "IX_MemberProfiles_Code" ON "MemberProfiles" ("Code");

CREATE TABLE "RekeyCandidates" (
    "Id" INTEGER NOT NULL,
    "Label" TEXT NOT NULL
);

CREATE TABLE "ActionParents" (
    "Id" INTEGER NOT NULL,
    CONSTRAINT "PK_ActionParents" PRIMARY KEY ("Id")
);

CREATE TABLE "ActionNoAction" (
    "Id" INTEGER NOT NULL,
    "ParentId" INTEGER NOT NULL,
    CONSTRAINT "FK_ActionNoAction_Parents" FOREIGN KEY ("ParentId") REFERENCES "ActionParents" ("Id") ON DELETE NO ACTION ON UPDATE NO ACTION,
    CONSTRAINT "PK_ActionNoAction" PRIMARY KEY ("Id")
);

CREATE TABLE "ActionRestrict" (
    "Id" INTEGER NOT NULL,
    "ParentId" INTEGER NOT NULL,
    CONSTRAINT "FK_ActionRestrict_Parents" FOREIGN KEY ("ParentId") REFERENCES "ActionParents" ("Id") ON DELETE RESTRICT ON UPDATE RESTRICT,
    CONSTRAINT "PK_ActionRestrict" PRIMARY KEY ("Id")
);

CREATE TABLE "ActionCascade" (
    "Id" INTEGER NOT NULL,
    "ParentId" INTEGER NOT NULL,
    CONSTRAINT "FK_ActionCascade_Parents" FOREIGN KEY ("ParentId") REFERENCES "ActionParents" ("Id") ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT "PK_ActionCascade" PRIMARY KEY ("Id")
);

CREATE TABLE "ActionSetNull" (
    "Id" INTEGER NOT NULL,
    "ParentId" INTEGER,
    CONSTRAINT "FK_ActionSetNull_Parents" FOREIGN KEY ("ParentId") REFERENCES "ActionParents" ("Id") ON DELETE SET NULL ON UPDATE SET NULL,
    CONSTRAINT "PK_ActionSetNull" PRIMARY KEY ("Id")
);

CREATE TABLE "ActionSetDefault" (
    "Id" INTEGER NOT NULL,
    "ParentId" INTEGER NOT NULL DEFAULT 1,
    CONSTRAINT "FK_ActionSetDefault_Parents" FOREIGN KEY ("ParentId") REFERENCES "ActionParents" ("Id") ON DELETE SET DEFAULT ON UPDATE SET DEFAULT,
    CONSTRAINT "PK_ActionSetDefault" PRIMARY KEY ("Id")
);

INSERT INTO "__EFMigrationsHistory" ("MigrationId", "ProductVersion")
VALUES ('20260729000100_Phase6Initial', '10.0.10');

COMMIT;

START TRANSACTION;
ALTER TABLE "RekeyCandidates" ADD CONSTRAINT "PK_RekeyCandidates_Physical" PRIMARY KEY ("Id");

ALTER TABLE "MemberProfiles" ALTER COLUMN "Rating" DROP DEFAULT;

ALTER TABLE "MemberProfiles" ALTER COLUMN "Rating" TYPE REAL;

ALTER TABLE "MemberProfiles" ALTER COLUMN "Rating" SET DEFAULT 2.0;

ALTER TABLE "MemberProfiles" ALTER COLUMN "Code" SET DEFAULT 'member';

ALTER TABLE "MemberProfiles" ALTER COLUMN "Code" SET COLLATION "NOCASE";

ALTER TABLE "MemberProfiles" ALTER COLUMN "Code" DROP NOT NULL;

ALTER TABLE "MemberProfiles" RENAME TO "Members";

ALTER TABLE "Members" RENAME COLUMN "Code" TO "Handle";

ALTER TABLE "Members" RENAME INDEX "IX_MemberProfiles_Code" TO "IX_Members_Handle";

INSERT INTO "__EFMigrationsHistory" ("MigrationId", "ProductVersion")
VALUES ('20260729000200_Phase6RewriteAndRename', '10.0.10');

COMMIT;

START TRANSACTION;
ALTER TABLE "RekeyCandidates" DROP CONSTRAINT "PK_RekeyCandidates_Physical";

ALTER TABLE "RekeyCandidates" ADD COLUMN "Region" TEXT NOT NULL DEFAULT 'west';

ALTER TABLE "RekeyCandidates" ADD CONSTRAINT "PK_RekeyCandidates" PRIMARY KEY ("Region", "Id");

INSERT INTO "__EFMigrationsHistory" ("MigrationId", "ProductVersion")
VALUES ('20260729000300_Phase6CompositeRekey', '10.0.10');

COMMIT;
