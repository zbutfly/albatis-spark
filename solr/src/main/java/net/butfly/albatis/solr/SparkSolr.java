package net.butfly.albatis.solr;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

import java.util.Map;

public interface SparkSolr {
	default Map<String, String> solrOpts(URISpec targetUri) {
		String solrdbn = targetUri.getPathAt(0);
		String solrcol = targetUri.getPathAt(1);
		String solruri = targetUri.getScheme() + "://" + targetUri.getAuthority() + "/" + solrdbn;
		Logger.getLogger(getClass()).info("SolrSpark connecting to: " + targetUri.toString());
		return Maps.of("uri", solruri //
				, "database", solrdbn//
				, "collection", null == solrcol ? "" : solrcol //
		);
	}
}
