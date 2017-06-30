<?php
/**
 * Created by PhpStorm.
 * User: minhao
 * Date: 2017-06-30
 * Time: 18:23
 */

namespace Oasis\Mlib\Redshift;

use Oasis\Mlib\AwsWrappers\StsClient;

class StsCredentialProvider implements CredentialProviderInterface
{
    /**
     * @var StsClient
     */
    private $stsClient;
    
    public function __construct(StsClient $stsClient)
    {
        $this->stsClient = $stsClient;
    }
    
    public function getCredentialString($durationInSeconds = 43200)
    {
        $tempCredential = $this->stsClient->getTemporaryCredential($durationInSeconds);
        
        return \sprintf(
            "CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s;token=%s'",
            $tempCredential->accessKeyId,
            $tempCredential->secretAccessKey,
            $tempCredential->sessionToken
        );
    }
}
