<?php
/**
 * Created by PhpStorm.
 * User: minhao
 * Date: 2016-01-11
 * Time: 17:22
 */

namespace Oasis\Mlib\Redshift;

use Doctrine\Common\EventManager;
use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Oasis\Mlib\AwsWrappers\TemporaryCredential;
use Oasis\Mlib\Utils\StringUtils;

class RedshiftConnection extends Connection
{
    /**
     * @param array              $params
     * @param Configuration|null $config
     * @param EventManager|null  $eventManager
     *
     * @return static
     * @throws \Doctrine\DBAL\DBALException
     */
    public static function getConnection(array $params,
                                         Configuration $config = null,
                                         EventManager $eventManager = null)
    {
        $params['wrapperClass'] = static::class;
        $params['driver']       = 'pdo_pgsql';

        return DriverManager::getConnection($params, $config, $eventManager);
    }

    public function copyFromS3($table,
                               $columns,
                               $s3path,
                               $s3region,
                               TemporaryCredential $tempCredential,
                               $escaped = true,
                               $gzip = false,
                               $maxerror = 0
    )
    {
        $stmt_template = <<<SQL
COPY %s (%s) FROM '%s'
CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s;token=%s'
REGION %s
%s
%s
%s

SQL;
        $stmt          = sprintf(
            $stmt_template,
            $this->normalizeTable($table),
            $this->normalizeColumns($columns),
            $this->normalizeS3Path($s3path),
            $tempCredential->accessKeyId,
            $tempCredential->secretAccessKey,
            $tempCredential->sessionToken,
            $this->normalizeSingleQuotedValue($s3region),
            ($escaped ? "ESCAPE" : ""),
            ($gzip ? "GZIP" : ""),
            ($maxerror > 0 ? "MAXERROR $maxerror" : "")
        );

        mdebug("Copying using stmt:\n%s", $stmt);
        $prepared_statement = $this->prepare($stmt);
        $prepared_statement->execute();
    }

    /**
     * NOTE: region of s3 bucket must be the same as redshift cluster
     *
     * @param                     $selectStatement
     * @param                     $s3path
     * @param TemporaryCredential $tempCredential
     * @param bool                $escaped
     * @param bool                $gzip
     * @param bool                $parallel
     *
     * @throws \Doctrine\DBAL\DBALException
     */
    public function unloadToS3($selectStatement,
                               $s3path,
                               TemporaryCredential $tempCredential,
                               $escaped = true,
                               $gzip = false,
                               $parallel = true
    )
    {
        $stmt_template = <<<SQL
UNLOAD (%s)
TO '%s'
CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s;token=%s'
%s
%s
%s
ALLOWOVERWRITE
SQL;
        $stmt          = sprintf(
            $stmt_template,
            $this->normalizeSingleQuotedValue($selectStatement),
            $this->normalizeS3Path($s3path),
            $tempCredential->accessKeyId,
            $tempCredential->secretAccessKey,
            $tempCredential->sessionToken,
            ($escaped ? "ESCAPE" : ""),
            ($gzip ? "GZIP" : ""),
            ($parallel ? "" : "PARALLEL OFF")
        );
        mdebug("Unloading using stmt:\n%s", $stmt);
        $prepared_statement = $this->prepare($stmt);
        $prepared_statement->execute();
    }

    public function normalizeTable($table)
    {
        $exploded = explode(".", $table);
        switch (sizeof($exploded)) {
            case 1:
                if (!(StringUtils::stringStartsWith($table, "\"")
                      && StringUtils::stringEndsWith($table, "\""))
                ) {
                    $table = "\"$table\"";
                }
                break;
            case 2:
                $schema = $exploded[0];
                if (!(StringUtils::stringStartsWith($schema, "\"")
                      && StringUtils::stringEndsWith($schema, "\""))
                ) {
                    $schema = "\"$schema\"";
                }
                $name = $exploded[1];
                if (!(StringUtils::stringStartsWith($name, "\"")
                      && StringUtils::stringEndsWith($name, "\""))
                ) {
                    $name = "\"$name\"";
                }
                $table = "$schema.$name";
                break;
            default:
                throw new \InvalidArgumentException("Invalid table name: $table");
                break;
        }

        return $table;
    }

    /**
     * Normalize columns into comma delitited list
     *
     * @param string|array $columns
     *
     * @return string
     */
    public function normalizeColumns($columns)
    {
        if (is_string($columns)) {
            $columns = explode(",", $columns);
        }
        elseif (!is_array($columns)) {
            throw new \InvalidArgumentException("Columns should be either an array, or a comma delimited string");
        }

        $clist = '';
        foreach ($columns as $c) {
            $c = trim($c);
            if (!$c) {
                continue;
            }

            if (!(StringUtils::stringStartsWith($c, "\"")
                  && StringUtils::stringEndsWith($c, "\""))
            ) {
                $c = "\"$c\"";
            }
            $clist .= ", $c";
        }
        $clist = trim($clist, ",");

        return $clist;
    }

    public function normalizeS3Path($s3path)
    {
        static $protocol = "s3://";
        if (StringUtils::stringStartsWith($s3path, $protocol)) {
            $s3path = substr($s3path, strlen($protocol));
        }
        $s3path = preg_replace('#/+#', "/", $s3path);

        return $protocol . $s3path;
    }

    public function normalizeSingleQuotedValue($value)
    {
        $value = addcslashes($value, "\\'");

        return "'$value'";
    }
}
