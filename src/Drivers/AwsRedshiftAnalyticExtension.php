<?php

namespace Oasis\Mlib\Redshift\Drivers;

use Doctrine\DBAL\DBALException;
use Oasis\Mlib\Redshift\CredentialProviderInterface;

class AwsRedshiftAnalyticExtension extends ConnectionAnalyticExtension
{
    public function copyFromS3(
        $table,
        $columns,
        $s3path,
        $s3region,
        CredentialProviderInterface $credentialProvider,
        $escaped = true,
        $gzip = false,
        $maxerror = 0,
        $options = []
    ) {
        $stmt_template = /** @lang text */
            <<<SQL
COPY %s (%s) FROM '%s'
%s
REGION %s
%s
%s
%s

SQL;
        $stmt          = sprintf(
            $stmt_template,
            $this->connection->normalizeTable($table),
            $this->connection->normalizeColumns($columns),
            $this->connection->normalizeS3Path($s3path),
            $credentialProvider->getCredentialString(),
            $this->connection->normalizeSingleQuotedValue($s3region),
            ($escaped ? "ESCAPE" : ""),
            ($gzip ? "GZIP" : ""),
            ($maxerror > 0 ? "MAXERROR $maxerror" : "")
        );

        if ($options) {
            foreach ($options as $argu) {
                $stmt .= " $argu";
            }
        }

        mdebug("Copying using stmt:\n%s", $stmt);
        $prepared_statement = $this->connection->prepare($stmt);
        $prepared_statement->execute();
    }

    /**
     * NOTE: region of s3 bucket must be the same as redshift cluster
     *
     * @param                             $selectStatement
     * @param                             $s3path
     * @param  CredentialProviderInterface  $credentialProvider
     * @param  bool  $escaped
     * @param  bool  $gzip
     * @param  bool  $parallel
     *
     * @param  array  $options
     * @throws DBALException
     */
    public function unloadToS3(
        $selectStatement,
        $s3path,
        CredentialProviderInterface $credentialProvider,
        $escaped = true,
        $gzip = false,
        $parallel = true,
        $options = []
    ) {
        $stmt_template = /** @lang text */
            <<<SQL
UNLOAD (%s)
TO '%s'
%s
%s
%s
%s
ALLOWOVERWRITE
SQL;
        $stmt          = sprintf(
            $stmt_template,
            $this->connection->normalizeSingleQuotedValue($selectStatement),
            $this->connection->normalizeS3Path($s3path),
            $credentialProvider->getCredentialString(),
            ($escaped ? "ESCAPE" : ""),
            ($gzip ? "GZIP" : ""),
            ($parallel ? "" : "PARALLEL OFF")
        );

        if ($options) {
            foreach ($options as $argu) {
                $stmt .= " $argu";
            }
        }

        mdebug("Unloading using stmt:\n%s", $stmt);
        $prepared_statement = $this->connection->prepare($stmt);
        $prepared_statement->execute();
    }
}
