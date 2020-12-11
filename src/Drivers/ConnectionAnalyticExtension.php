<?php


namespace Oasis\Mlib\Redshift\Drivers;

use Doctrine\DBAL\DBALException;
use Oasis\Mlib\Redshift\CredentialProviderInterface;
use Oasis\Mlib\Redshift\RedshiftConnection;

abstract class ConnectionAnalyticExtension
{
    /** @var RedshiftConnection */
    protected $connection;

    public function __construct(RedshiftConnection $connection)
    {
        $this->connection = $connection;
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
    abstract public function copyFromS3(
        $table,
        $columns,
        $s3path,
        $s3region,
        CredentialProviderInterface $credentialProvider,
        $escaped = true,
        $gzip = false,
        $maxerror = 0,
        $options = []
    );

    /**
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
    abstract public function unloadToS3(
        $selectStatement,
        $s3path,
        CredentialProviderInterface $credentialProvider,
        $escaped = true,
        $gzip = false,
        $parallel = true,
        $options = []
    );

}
