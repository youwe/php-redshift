<?php
/**
 * Created by PhpStorm.
 * User: minhao
 * Date: 2016-08-17
 * Time: 14:49
 */

namespace Oasis\Mlib\Redshift;

use Oasis\Mlib\FlysystemWrappers\ExtendedFilesystem;

class RedshiftImporter
{
    /**
     * @var RedshiftConnection
     */
    private $connection;
    /**
     * @var ExtendedFilesystem
     */
    private $s3Fs;
    /**
     * @var ExtendedFilesystem
     */
    private $localFs;
    private $s3Region;
    /**
     * @var CredentialProviderInterface
     */
    private $credentialProvider;
    
    public function __construct(RedshiftConnection $connection,
                                ExtendedFilesystem $localFs,
                                ExtendedFilesystem $s3Fs,
                                $s3Region,
                                CredentialProviderInterface $credentialProvider)
    {
        $this->connection         = $connection;
        $this->localFs            = $localFs;
        $this->s3Fs               = $s3Fs;
        $this->s3Region           = $s3Region;
        $this->credentialProvider = $credentialProvider;
    }
    
    public function importFromS3($path, $table, $columns, $gzip = false)
    {
        $s3path = $this->s3Fs->getRealpath($path);
        mdebug("Will import files from s3 prefix %s", $s3path);
        
        $this->connection->copyFromS3(
            $table,
            $columns,
            $s3path,
            $this->s3Region,
            $this->credentialProvider,
            true,
            $gzip
        );
    }
    
    public function importFromFile($path, $table, $columns, $gzip = false, $overwriteS3Files = false)
    {
        $timestamp = microtime(true) . getmypid();
        $path      = ltrim(preg_replace('#/+#', "/", $path), "/");
        
        $suffix        = $gzip ? "\\.gz" : "";
        $uploadPattern = \sprintf(
            "#^%s([0-9]{2,})?(_part_[0-9]{2,})?%s\$#",
            \preg_quote($path, '#'),
            $suffix
        );
        $clearPattern  = \sprintf(
            "#^%s/%s\$#",
            $timestamp,
            preg_quote($path, "#")
        );
        mdebug("Upload pattern is %s", $uploadPattern);
        mdebug("Clear pattern is %s", $clearPattern);
        
        $localFinder = $this->localFs->getFinder();
        $localFinder->path($uploadPattern);
        if ($localFinder->count() == 0) {
            throw new \RuntimeException(
                sprintf("No import files found at path: %s, pattern = %s", $path, $uploadPattern)
            );
        }
        
        try {
            $s3Finder = $this->s3Fs->getFinder();
            $s3Finder->path($clearPattern);
            if ($s3Finder->count() > 0) {
                if ($overwriteS3Files) {
                    foreach ($s3Finder as $splFileInfo) {
                        $this->s3Fs->delete($splFileInfo->getRelativePathname());
                    }
                }
                else {
                    throw new \RuntimeException(sprintf("The path is not empty on remote end, path = %s", $path));
                }
            }
        } catch (\InvalidArgumentException $e) {
            if (strpos($e->getMessage(), "directory does not exist") === false) {
                throw $e;
            }
        }
        
        $uploaded = [];
        try {
            foreach ($localFinder as $splFileInfo) {
                $relativePathname = $splFileInfo->getRelativePathname();
                $remoteName       = \sprintf("%s/%s", $timestamp, $relativePathname);
                $fh               = $this->localFs->readStream($relativePathname);
                // IMPORTANT: putStream will not check for file existence, while writeStream will check
                // calling an has() before putting actual content will break strong consistency of S3
                $this->s3Fs->putStream($remoteName, $fh);
                fclose($fh);
                $uploaded[] = $remoteName;
                
                mdebug("Uploaded %s to %s", $relativePathname, $remoteName);
            }
            
            $this->importFromS3(\sprintf("%s/%s", $timestamp, $path), $table, $columns, $gzip);
        } finally {
            foreach ($uploaded as $relativePathname) {
                $this->s3Fs->delete($relativePathname);
            }
        }
    }
}
