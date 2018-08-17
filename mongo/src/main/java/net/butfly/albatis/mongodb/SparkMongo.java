package net.butfly.albatis.mongodb;

import java.util.Map;

import net.butfly.albacore.io.URISpec;
import net.butfly.albacore.utils.collection.Maps;
import net.butfly.albacore.utils.logger.Logger;

public interface SparkMongo {
	default Map<String, String> mongoOpts(URISpec targetUri) {
		String mongodbn = targetUri.getPathAt(0);
		String mongocol = targetUri.getPathAt(1);
		String mongouri = targetUri.getScheme() + "://" + targetUri.getAuthority() + "/" + mongodbn;
		Logger.getLogger(getClass()).info("MongoSpark connecting to: " + targetUri.toString());
		return Maps.of("uri", mongouri //
				, "database", mongodbn//
				, "collection", null == mongocol ? "" : mongocol //
		);
	}
}
