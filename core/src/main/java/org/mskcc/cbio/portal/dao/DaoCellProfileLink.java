/*
 * Copyright (c) 2017 The Hyve B.V.
 * This code is licensed under the GNU Affero Cellral Public License (AGPL),
 * version 3, or (at your option) any later version.
 */

/*
 * This file is part of cBioPortal.
 *
 * cBioPortal is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero Cellral Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero Cellral Public License for more details.
 *
 * You should have received a copy of the GNU Affero Cellral Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

/*
 * @author Sander Tan
*/

package org.mskcc.cbio.portal.dao;

import org.mskcc.cbio.portal.model.CellProfileLink;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DaoCellProfileLink {
	
	private DaoCellProfileLink() {
	}
	
	/**
     * Set cell profile link in `IM_cell_profile_link` table in database.
     * @throws DaoException 
     */
    public static void addCellProfileLink(CellProfileLink cellProfileLink) throws DaoException {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        
        try {
        	// Open connection to database
            connection = JdbcUtil.getDbConnection(DaoCellProfileLink.class);
	        
	        // Prepare SQL statement
            preparedStatement = connection.prepareStatement("INSERT INTO IM_cell_profile_link " 
	                + "(REFERRING_CELL_PROFILE_ID, REFERRED_CELL_PROFILE_ID, REFERENCE_TYPE) VALUES(?,?,?)");	        
            
            // Fill in statement
            preparedStatement.setInt(1, cellProfileLink.getReferringCellProfileId());
            preparedStatement.setInt(2, cellProfileLink.getReferredCellProfileId());
            preparedStatement.setString(3, cellProfileLink.getReferenceType());
            
            // Execute statement
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoCellProfileLink.class, connection, preparedStatement, resultSet);
        }
    }
}
