package org.mskcc.cbio.portal.dao;

import org.mskcc.cbio.portal.model.PortalGenePanel;
import org.mskcc.cbio.portal.model.PortalGenePanelGene;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DaoPortalGenePanel {

    private DaoPortalGenePanel() {
    }

    public static List<PortalGenePanel> getAllGenePanels(Integer pageSize, Integer pageNumber) throws DaoException {
        List<PortalGenePanel> genePanels = fetchGenePanels(pageSize, pageNumber);
        Map<String, PortalGenePanel> panelsByStableId = genePanels.stream()
            .filter(panel -> panel.getGenePanelId() != null)
            .collect(Collectors.toMap(PortalGenePanel::getGenePanelId, panel -> panel));

        if (!panelsByStableId.isEmpty()) {
            Map<String, List<PortalGenePanelGene>> genesByPanelId =
                fetchGenesForPanels(panelsByStableId.keySet());
            for (PortalGenePanel panel : panelsByStableId.values()) {
                List<PortalGenePanelGene> genes = genesByPanelId.get(panel.getGenePanelId());
                panel.setGenes(genes == null ? Collections.emptyList() : genes);
            }
        }

        return genePanels;
    }

    private static List<PortalGenePanel> fetchGenePanels(Integer pageSize, Integer pageNumber) throws DaoException {
        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        List<PortalGenePanel> genePanels = new ArrayList<>();
        try {
            con = JdbcUtil.getDbConnection(DaoPortalGenePanel.class);
            Integer offset = calculateOffset(pageSize, pageNumber);
            boolean applyLimit = pageSize != null && pageSize != 0 && offset != null;

            String sql = "SELECT gene_panel.internal_id AS internal_id, "
                + "gene_panel.stable_id AS gene_panel_id, "
                + "gene_panel.description AS description "
                + "FROM gene_panel";
            if (applyLimit) {
                sql += " LIMIT ? OFFSET ?";
            }

            pstmt = con.prepareStatement(sql);
            if (applyLimit) {
                pstmt.setInt(1, pageSize);
                pstmt.setInt(2, offset);
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                PortalGenePanel genePanel = new PortalGenePanel();
                genePanel.setInternalId(getNullableInteger(rs, "internal_id"));
                genePanel.setGenePanelId(rs.getString("gene_panel_id"));
                genePanel.setDescription(rs.getString("description"));
                genePanels.add(genePanel);
            }
            return genePanels;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoPortalGenePanel.class, con, pstmt, rs);
        }
    }

    private static Map<String, List<PortalGenePanelGene>> fetchGenesForPanels(Set<String> stableIds) throws DaoException {
        if (stableIds.isEmpty()) {
            return Collections.emptyMap();
        }

        Connection con = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        Map<String, List<PortalGenePanelGene>> genesByPanelId = new HashMap<>();
        try {
            con = JdbcUtil.getDbConnection(DaoPortalGenePanel.class);

            String placeholders = stableIds.stream().map(id -> "?").collect(Collectors.joining(","));
            String sql = "SELECT gene_panel.stable_id AS gene_panel_id, "
                + "gene_panel_list.gene_id AS entrez_gene_id, "
                + "gene.hugo_gene_symbol AS hugo_gene_symbol "
                + "FROM gene_panel_list "
                + "INNER JOIN gene_panel ON gene_panel_list.internal_id = gene_panel.internal_id "
                + "INNER JOIN gene ON gene_panel_list.gene_id = gene.entrez_gene_id "
                + "WHERE gene_panel.stable_id IN (" + placeholders + ") "
                + "ORDER BY hugo_gene_symbol ASC";

            pstmt = con.prepareStatement(sql);
            int index = 1;
            for (String stableId : stableIds) {
                pstmt.setString(index++, stableId);
            }
            rs = pstmt.executeQuery();
            while (rs.next()) {
                PortalGenePanelGene gene = new PortalGenePanelGene();
                gene.setGenePanelId(rs.getString("gene_panel_id"));
                gene.setEntrezGeneId(getNullableInteger(rs, "entrez_gene_id"));
                gene.setHugoGeneSymbol(rs.getString("hugo_gene_symbol"));
                genesByPanelId
                    .computeIfAbsent(gene.getGenePanelId(), key -> new ArrayList<>())
                    .add(gene);
            }
            return genesByPanelId;
        } catch (SQLException e) {
            throw new DaoException(e);
        } finally {
            JdbcUtil.closeAll(DaoPortalGenePanel.class, con, pstmt, rs);
        }
    }

    private static Integer calculateOffset(Integer pageSize, Integer pageNumber) {
        if (pageSize == null || pageNumber == null) {
            return null;
        }
        return pageSize * pageNumber;
    }

    private static Integer getNullableInteger(ResultSet rs, String column) throws SQLException {
        int value = rs.getInt(column);
        if (rs.wasNull()) {
            return null;
        }
        return value;
    }
}
