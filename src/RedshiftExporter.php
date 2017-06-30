<?php
/**
 * Created by PhpStorm.
 * User: minhao
 * Date: 2016-08-17
 * Time: 14:49
 */

namespace Oasis\Mlib\Redshift;

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
    
    public function exportToS3($path,
                               $query,
                               $gzip = false,
                               $isParallel = true,
                               $overwriteExistingFiles = false)
    {
        $s3path = $this->s3Fs->getRealpath($path);
        mdebug("export file to temp s3, prefix = %s", $s3path);
        
        // clear remote path
        try {
            $clearPattern = \sprintf("#^%s#", \preg_quote($path, "#"));
            mdebug("Clear pattern for %s is %s", $s3path, $clearPattern);
            $finder       = $this->s3Fs->getFinder();
            $finder->path($clearPattern);
            if ($finder->count() > 0) {
                if ($overwriteExistingFiles) {
                    foreach ($finder as $splFileInfo) {
                        $this->s3Fs->delete($splFileInfo->getRelativePathname());
                    }
                }
                else {
                    throw new \RuntimeException(sprintf("The path is not empty on remote end, path = %s", $s3path));
                }
            }
        } catch (\InvalidArgumentException $e) {
            if (strpos($e->getMessage(), "directory does not exist") === false) {
                throw $e;
            }
        }
        
        $this->connection->unloadToS3(
            $query,
            $s3path,
            $this->credentialProvider,
            true,
            $gzip,
            $isParallel
        );
    }
    
    public function exportToFile($path,
                                 $query,
                                 $gzip = false,
                                 $isParallel = true,
                                 $overwriteExistingFiles = false)
    {
        
        $path = ltrim(preg_replace('#/+#', "/", $path), "/");
        
        $suffix          = $gzip ? "\\.gz" : "";
        $clearPattern    = \sprintf("#^%s#", \preg_quote($path, "#"));
        $downloadPattern = \sprintf("#^%s([0-9]{2,})?(_part_[0-9]{2,})?%s\$#", \preg_quote($path, "#"), $suffix);
        mdebug("Clear pattern for %s is %s", $path, $clearPattern);
        mdebug("Download pattern for %s is %s", $path, $downloadPattern);
        
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
        
        $this->exportToS3($path, $query, $gzip, $isParallel, $overwriteExistingFiles);
        
        $finder = $this->s3Fs->getFinder();
        $finder->path($downloadPattern);
        foreach ($finder as $splFileInfo) {
            //var_dump($splFileInfo->getRelativePathname());
            $partName = $splFileInfo->getRelativePathname();
            $fh       = $this->s3Fs->readStream($partName);
            $this->localFs->putStream($partName, $fh);
            fclose($fh);
            $this->s3Fs->delete($partName);
        }
        
    }
}
