<?php
/**
 * Created by PhpStorm.
 * User: minhao
 * Date: 2017-06-30
 * Time: 18:26
 */

namespace Oasis\Mlib\Redshift;

class IAMRoleCredentialProvider implements CredentialProviderInterface
{
    /**
     * @var
     */
    private $roleArn;
    
    public function __construct($roleArn)
    {
        $this->roleArn = $roleArn;
    }
    
    public function getCredentialString()
    {
        return \sprintf("iam_role '%s'", $this->roleArn);
    }
}
