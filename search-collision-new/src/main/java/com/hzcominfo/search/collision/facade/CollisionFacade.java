package com.hzcominfo.search.collision.facade;

import com.hzcominfo.search.collision.dto.CollisionResponse;
import com.hzcominfo.search.collision.dto.Collisionquest;

import net.butfly.albacore.exception.BusinessException;
import net.butfly.albacore.facade.Facade;
import net.butfly.bus.TX;

public interface CollisionFacade extends Facade {

	@TX("SEARCH_COLLISION_001")
	CollisionResponse submit(Collisionquest model) throws BusinessException;
	
	@TX("SEARCH_COLLISION_002")
	CollisionResponse query(Collisionquest model, int currPage, int pageSize) throws BusinessException;
	
	@TX("SEARCH_COLLISION_003")
	CollisionResponse cancel(Collisionquest model) throws BusinessException;
	
	@TX("SEARCH_COLLISION_004")
	CollisionResponse export(Collisionquest model)  throws BusinessException;
}
