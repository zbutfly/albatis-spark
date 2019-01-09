package net.butfly.albatis.elastic;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

import java.util.Map;

public interface SparkESInterface {
	default Map<String, String> esOpts(URISpec targetUri) {
		String esdbn = targetUri.getPathAt(0);
		String escol = targetUri.getPathAt(1);
		String esuri = targetUri.getScheme() + "://" + targetUri.getAuthority() + "/" + esdbn;
		Logger.getLogger(getClass()).info("ESSpark connecting to: " + targetUri.toString());
		return Maps.of("uri", esuri //
				, "database", esuri//
				, "collection", null == escol ? "" : escol //
		);
	}
}
