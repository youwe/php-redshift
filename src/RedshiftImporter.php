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
    
    public function importFromFile($path, $table, $columns, $gzip = false, $overwriteS3Files = false)
    {
        $path          = ltrim(preg_replace('#/+#', "/", $path), "/");
        $suffix        = $gzip ? "\\.gz" : "";
        $uploadPattern = "#^" . preg_quote($path, "#") . "([0-9]{2,})?(_part_[0-9]{2,})?$suffix\$#";
        $clearPattern  = "#^" . preg_quote($path, "#") . "#";
        
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
        foreach ($localFinder as $splFileInfo) {
            $relativePathname = $splFileInfo->getRelativePathname();
            $fh               = $this->localFs->readStream($relativePathname);
            $this->s3Fs->putStream($relativePathname, $fh);
            fclose($fh);
            $uploaded[] = $relativePathname;
        }
        
        //$inStream = $this->localFs->readStream($path);
        //$this->s3Fs->putStream($path, $inStream);
        
        $s3path = $this->s3Fs->getRealpath($path);
        mdebug("uploaded file to temp s3, path = %s", $s3path);
        
        $tempCredential = $this->sts->getTemporaryCredential();
        
        $this->connection->copyFromS3(
            $table,
            $columns,
            $s3path,
            $this->s3Region,
            $tempCredential,
            true,
            $gzip
        );
        
        foreach ($uploaded as $relativePathname) {
            $this->s3Fs->delete($relativePathname);
        }
    }
}
