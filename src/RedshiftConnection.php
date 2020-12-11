<?php /** @noinspection SyntaxError */

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
use Doctrine\DBAL\DBALException;
use Doctrine\DBAL\DriverManager;
use Oasis\Mlib\Redshift\Drivers\AwsRedshiftAnalyticExtension;
use Oasis\Mlib\Redshift\Drivers\ConnectionAnalyticExtension;
use Oasis\Mlib\Utils\Exceptions\DataValidationException;
use Oasis\Mlib\Utils\StringUtils;

/**
 * Class RedshiftConnection
 * @package Oasis\Mlib\Redshift
 */
class RedshiftConnection extends Connection
{

    /** @var ConnectionAnalyticExtension */
    protected $analyticExtension;

    /**
     * @param  array  $params
     * @param  Configuration|null  $config
     * @param  EventManager|null  $eventManager
     *
     * @return Connection|RedshiftConnection
     * @throws DBALException
     */
    public static function getConnection(
        array $params,
        Configuration $config = null,
        EventManager $eventManager = null
    ) {
        $params['wrapperClass'] = static::class;
        $params['driver']       = 'pdo_pgsql';

        /** @var RedshiftConnection $cnn */
        $cnn = DriverManager::getConnection($params, $config, $eventManager);

        $cnn->analyticExtension = $params['analytic_ext'] ?? new AwsRedshiftAnalyticExtension($cnn);
        if (($cnn->analyticExtension instanceof ConnectionAnalyticExtension) === false) {
            throw new DataValidationException("Bad class type in 'analytic_ext'");
        }

        return $cnn;
    }

    /**
     * @param $table
     * @param $columns
     * @param $s3path
     * @param $s3region
     * @param  CredentialProviderInterface  $credentialProvider
     * @param  bool  $escaped
     * @param  bool  $gzip
     * @param  int  $maxerror
     * @param  array  $options
     * @throws DBALException
     */
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
        $this->analyticExtension->copyFromS3(
            $table,
            $columns,
            $s3path,
            $s3region,
            $credentialProvider,
            $escaped,
            $gzip,
            $maxerror,
            $options
        );
    }

    /**
     *
     * @param  $selectStatement
     * @param  $s3path
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
        $this->analyticExtension->unloadToS3(
            $selectStatement,
            $s3path,
            $credentialProvider,
            $escaped,
            $gzip,
            $parallel,
            $options
        );
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
     * @param  string|array  $columns
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

        return $protocol.$s3path;
    }

    public function normalizeSingleQuotedValue($value)
    {
        $value = addcslashes($value, "\\'");

        return "'$value'";
    }
}
