START TRANSACTION;
ALTER TABLE "RekeyCandidates" DROP CONSTRAINT "PK_RekeyCandidates";

ALTER TABLE "RekeyCandidates" DROP COLUMN "Region";

ALTER TABLE "RekeyCandidates" ADD CONSTRAINT "PK_RekeyCandidates_Physical" PRIMARY KEY ("Id");

DELETE FROM "__EFMigrationsHistory"
WHERE "MigrationId" = '20260729000300_Phase6CompositeRekey';

COMMIT;

START TRANSACTION;
ALTER TABLE "Members" RENAME INDEX "IX_Members_Handle" TO "IX_MemberProfiles_Code";

ALTER TABLE "Members" RENAME COLUMN "Handle" TO "Code";

ALTER TABLE "Members" RENAME TO "MemberProfiles";

ALTER TABLE "MemberProfiles" ALTER COLUMN "Code" SET DEFAULT 'legacy';

ALTER TABLE "MemberProfiles" ALTER COLUMN "Code" DROP COLLATION;

ALTER TABLE "MemberProfiles" ALTER COLUMN "Code" SET NOT NULL;

ALTER TABLE "MemberProfiles" ALTER COLUMN "Rating" DROP DEFAULT;

ALTER TABLE "MemberProfiles" ALTER COLUMN "Rating" TYPE INTEGER;

ALTER TABLE "MemberProfiles" ALTER COLUMN "Rating" SET DEFAULT 1;

ALTER TABLE "RekeyCandidates" DROP CONSTRAINT "PK_RekeyCandidates_Physical";

DELETE FROM "__EFMigrationsHistory"
WHERE "MigrationId" = '20260729000200_Phase6RewriteAndRename';

COMMIT;

START TRANSACTION;
DROP TABLE "ActionCascade";

DROP TABLE "ActionNoAction";

DROP TABLE "ActionRestrict";

DROP TABLE "ActionSetDefault";

DROP TABLE "ActionSetNull";

DROP TABLE "WorkItems";

DROP TABLE "ActionParents";

DROP TABLE "MemberProfiles";

DROP TABLE "Organizations";

DROP TABLE "RekeyCandidates";

DELETE FROM "__EFMigrationsHistory"
WHERE "MigrationId" = '20260729000100_Phase6Initial';

COMMIT;
