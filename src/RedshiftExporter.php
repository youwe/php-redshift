<?php
/**
 * Created by PhpStorm.
 * User: minhao
 * Date: 2016-08-17
 * Time: 14:49
 */

namespace Oasis\Mlib\Redshift;

use Oasis\Mlib\AwsWrappers\StsClient;
use Oasis\Mlib\FlysystemWrappers\ExtendedFilesystem;

class RedshiftExporter
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
    /**
     * @var StsClient
     */
    private $sts;
    private $s3Region;
    
    public function __construct(RedshiftConnection $connection,
                                ExtendedFilesystem $localFs,
                                ExtendedFilesystem $s3Fs,
                                $s3Region,
                                StsClient $sts)
    {
        $this->connection = $connection;
        $this->localFs    = $localFs;
        $this->s3Fs       = $s3Fs;
        $this->s3Region   = $s3Region;
        $this->sts        = $sts;
    }
    
    public function exportToFile($path,
                                 $query,
                                 $gzip = false,
                                 $isParallel = true,
                                 $overwriteExistingFiles = false)
    {
        
        $path   = ltrim(preg_replace('#/+#', "/", $path), "/");
        $s3path = $this->s3Fs->getRealpath($path);
        mdebug("export file to temp s3, prefix = %s", $s3path);
        
        $suffix          = $gzip ? "\\.gz" : "";
        $clearPattern    = "#^" . preg_quote($path, "#") . "#";
        $downloadPattern = "#^" . preg_quote($path, "#") . "([0-9]{2,})?(_part_[0-9]{2,})?$suffix\$#";
        mdebug("Clear pattern for %s is %s", $path, $clearPattern);
        mdebug("Download pattern for %s is %s", $path, $downloadPattern);
        
        // clear remote path
        $finder = $this->s3Fs->getFinder();
        $finder->path($clearPattern);
        if ($finder->count() > 0) {
            if ($overwriteExistingFiles) {
                foreach ($finder as $splFileInfo) {
                    $this->s3Fs->delete($splFileInfo->getRelativePathname());
                }
            }
            else {
                throw new \RuntimeException(sprintf("The path is not empty on remote end, path = %s", $path));
            }
        }
        // clear local path
        $finder = $this->localFs->getFinder();
        $finder->path($clearPattern);
        if ($finder->count() > 0) {
            if ($overwriteExistingFiles) {
                foreach ($finder as $splFileInfo) {
                    $this->localFs->delete($splFileInfo->getRelativePathname());
                }
            }
            else {
                throw new \RuntimeException(sprintf("The path is not empty locally, path = %s", $path));
            }
        }
        
        $tempCredential = $this->sts->getTemporaryCredential();
        
        $this->connection->unloadToS3(
            $query,
            $s3path,
            $tempCredential,
            true,
            $gzip,
            $isParallel
        );
        
        $finder = $this->s3Fs->getFinder();
        $finder->path($downloadPattern);
        foreach ($finder as $splFileInfo) {
            //var_dump($splFileInfo->getRelativePathname());
            $partName = $splFileInfo->getRelativePathname();
            $fh       = $this->s3Fs->readStream($partName);
            $this->localFs->putStream($partName, $fh);
            fclose($fh);
        }
        
    }
}
