package cn.schoolwow.quickdao.dao.dcl;

import cn.schoolwow.quickdao.domain.external.QuickDAOConfig;
import cn.schoolwow.quickdao.domain.external.dcl.GrantOption;

public class OracleDatabaseControl extends AbstractDabaseControl {

    public OracleDatabaseControl(QuickDAOConfig quickDAOConfig) {
        super(quickDAOConfig);
    }

    @Override
    public void createUserAndGrant(GrantOption grantOption) {
        createUser(grantOption.dataBaseUser);
        grant(grantOption);
    }
}